// esp32_mdns_stream.ino
//
// OV7670 (no FIFO) -> ESP32 I2S DMA capture -> UDP unicast, destination found
// via mDNS -> the PC currently running udp_mjpeg_server.py
//
// udp_mjpeg_server.py registers itself on the network as an mDNS service
// named "_ov7670._udp.local." using Python's `zeroconf` package (a pure
// -Python mDNS responder -- it does NOT depend on Bonjour/mDNSResponder being
// installed on the host, so this works on a bare Windows PC). This firmware
// asks "who provides _ov7670._udp on this network?" and sends frames to
// whatever IP:port answers -- so a fresh Windows machine, or a machine that
// picked up a different DHCP lease since last time, is found automatically.
//
// A background task keeps searching until it finds the server, then rechecks
// periodically in case the server restarts (e.g. a new shift, PC rebooted).
// While no server is known, captured frames are simply dropped -- exactly
// the same "never stall the pipeline" rule as everywhere else in this project.
//
// Tradeoff vs the broadcast variant (esp32_broadcast_stream.ino): more
// moving parts (the server must run zeroconf, mDNS multicast must be allowed
// on the network) but works even on networks that filter general broadcast,
// since mDNS's own multicast group is usually treated separately by network
// policy.
//
// Requires the rest of bitluni's library files (OV7670.*, I2SCamera.*, ...)
// from https://github.com/bitluni/ESP32CameraI2S in the same sketch folder.

#include <WiFi.h>
#include <WiFiUdp.h>
#include <ESPmDNS.h>
#include "OV7670.h"

// ---------------------------------------------------------------- user config

static const char *WIFI_SSID = "BeWareWifi";
static const char *WIFI_PASS = "Arsh9359529918";

// Must match the mDNS service type/name the Python side registers
// (service="ov7670", proto="udp" -> "_ov7670._udp.local.").
static const char *MDNS_SERVICE = "ov7670";
static const char *MDNS_PROTO = "udp";

// This ESP32's own mDNS identity. Only needs to be unique on the subnet.
static const char *MY_MDNS_NAME = "ov7670cam";

static const uint16_t LOCAL_PORT = 5006;
static const uint16_t CHUNK_PAYLOAD = 1400; // must match --chunk on the receiver
static const uint32_t PACKET_GAP_US = 100;

static const uint32_t DISCOVERY_RETRY_MS = 5000;    // while no server is known
static const uint32_t DISCOVERY_REFRESH_MS = 30000; // periodic recheck once known

#define CAMERA_MODE OV7670::Mode::QQVGA_RGB565

// ------------------------------------------------------------------ pin map

static const int SIOD = 21, SIOC = 22;
static const int VSYNC = 34, HREF = 35, XCLK = 32, PCLK = 33;
static const int D0 = 27, D1 = 17, D2 = 16, D3 = 15, D4 = 14, D5 = 13, D6 = 26, D7 = 4;

// ------------------------------------------------------------- wire protocol

struct __attribute__((packed)) PacketHeader
{
  char magic[2];
  uint16_t frameId;
  uint16_t chunkId;
  uint16_t totalChunks;
  uint16_t width;
  uint16_t height;
};

// ------------------------------------------------------------- shared state

struct FrameMsg
{
  uint8_t slot;
  uint16_t frameId;
};

static OV7670 *camera = nullptr;
static WiFiUDP udp;

static uint8_t *slots[2] = {nullptr, nullptr};
static size_t frameBytes = 0;

static QueueHandle_t freeQ = nullptr;
static QueueHandle_t readyQ = nullptr;

static volatile uint32_t framesCaptured = 0;
static volatile uint32_t framesSent = 0;
static volatile uint32_t framesDropped = 0;

static uint8_t txBuf[sizeof(PacketHeader) + CHUNK_PAYLOAD];

// ---------------------------------------------------------- server discovery
// Guarded by a mutex since it's written by discoveryTask (core 0, low
// priority) and read by senderTask (core 0, high priority) -- IPAddress is
// multiple bytes, so an unguarded read could tear mid-update.

static SemaphoreHandle_t serverLock = nullptr;
static IPAddress serverIP;
static uint16_t serverPort = 0;
static bool serverKnown = false;

static bool tryDiscoverServer()
{
  int n = MDNS.queryService(MDNS_SERVICE, MDNS_PROTO);
  Serial.printf("mDNS: queryService(_%s._%s) -> %d result(s)\n",
                MDNS_SERVICE, MDNS_PROTO, n);
  if (n <= 0)
    return false;

  // Log every answer so we can see which fields the responder actually filled.
  for (int i = 0; i < n; i++)
  {
    Serial.printf("  [%d] host=%s addr=%s port=%u\n", i,
                  MDNS.hostname(i).c_str(),
                  MDNS.address(i).toString().c_str(),
                  MDNS.port(i));
  }

  IPAddress ip = MDNS.address(0);
  uint16_t prt = MDNS.port(0);

  // ESP32 core 3.x / IDF mDNS often returns a valid host + port from a PTR
  // query but leaves the A record unresolved (0.0.0.0) when the service is
  // advertised by a non-Apple responder like Python zeroconf. Fall back to an
  // explicit host (A record) lookup in that case.
  if ((uint32_t)ip == 0)
  {
    String host = MDNS.hostname(0);
    if (host.length())
    {
      Serial.printf("mDNS: addr unresolved, querying host '%s' ...\n",
                    host.c_str());
      ip = MDNS.queryHost(host, 2000);
      Serial.printf("mDNS: queryHost -> %s\n", ip.toString().c_str());
    }
  }

  if ((uint32_t)ip == 0 || prt == 0)
  {
    Serial.println("mDNS: incomplete answer (no usable IP/port) -- will retry.");
    return false;
  }

  xSemaphoreTake(serverLock, portMAX_DELAY);
  serverIP = ip;
  serverPort = prt;
  serverKnown = true;
  xSemaphoreGive(serverLock);

  Serial.printf("mDNS: found server at %s:%u\n",
                serverIP.toString().c_str(), serverPort);
  return true;
}

static void discoveryTask(void *)
{
  for (;;)
  {
    bool knownNow;
    xSemaphoreTake(serverLock, portMAX_DELAY);
    knownNow = serverKnown;
    xSemaphoreGive(serverLock);

    if (!knownNow)
    {
      Serial.println("mDNS: searching for _ov7670._udp.local ...");
      tryDiscoverServer();
      vTaskDelay(pdMS_TO_TICKS(DISCOVERY_RETRY_MS));
    }
    else
    {
      // Server already known -- just recheck occasionally in case it moved
      // (new DHCP lease, different PC, restarted with a new IP).
      vTaskDelay(pdMS_TO_TICKS(DISCOVERY_REFRESH_MS));
      tryDiscoverServer();
    }
  }
}

// ------------------------------------------------------------- capture task

static void captureTask(void *)
{
  uint16_t frameId = 0;
  for (;;)
  {
    uint8_t slot;
    if (xQueueReceive(freeQ, &slot, portMAX_DELAY) != pdTRUE)
      continue;

    camera->oneFrame();
    memcpy(slots[slot], camera->frame, frameBytes);

    FrameMsg msg = {slot, frameId++};
    framesCaptured++;

    if (xQueueSend(readyQ, &msg, 0) != pdTRUE)
    {
      framesDropped++;
      xQueueSend(freeQ, &slot, 0);
    }
  }
}

// -------------------------------------------------------------- sender task

static void senderTask(void *)
{
  for (;;)
  {
    FrameMsg msg;
    if (xQueueReceive(readyQ, &msg, portMAX_DELAY) != pdTRUE)
      continue;

    IPAddress dest;
    uint16_t port;
    bool known;
    xSemaphoreTake(serverLock, portMAX_DELAY);
    known = serverKnown;
    dest = serverIP;
    port = serverPort;
    xSemaphoreGive(serverLock);

    if (!known || WiFi.status() != WL_CONNECTED)
    {
      // No known receiver yet (or WiFi is down) -- drop rather than stall.
      framesDropped++;
      xQueueSend(freeQ, &msg.slot, portMAX_DELAY);
      continue;
    }

    const uint8_t *src = slots[msg.slot];
    const uint16_t totalChunks =
        (uint16_t)((frameBytes + CHUNK_PAYLOAD - 1) / CHUNK_PAYLOAD);

    PacketHeader *hdr = (PacketHeader *)txBuf;
    hdr->magic[0] = 'O';
    hdr->magic[1] = 'V';
    hdr->frameId = msg.frameId;
    hdr->totalChunks = totalChunks;
    hdr->width = camera->xres;
    hdr->height = camera->yres;

    for (uint16_t c = 0; c < totalChunks; c++)
    {
      const size_t offset = (size_t)c * CHUNK_PAYLOAD;
      const size_t len = min((size_t)CHUNK_PAYLOAD, frameBytes - offset);

      hdr->chunkId = c;
      memcpy(txBuf + sizeof(PacketHeader), src + offset, len);

      udp.beginPacket(dest, port);
      udp.write(txBuf, sizeof(PacketHeader) + len);
      udp.endPacket();

      if (PACKET_GAP_US)
        delayMicroseconds(PACKET_GAP_US);
    }

    framesSent++;
    xQueueSend(freeQ, &msg.slot, portMAX_DELAY);
  }
}

// -------------------------------------------------------------------- setup

void setup()
{
  Serial.begin(115200);
  delay(200);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(300);
    Serial.print(".");
  }
  Serial.println();
  Serial.printf("ESP32 IP: %s\n", WiFi.localIP().toString().c_str());

  WiFi.setSleep(false);

  if (!MDNS.begin(MY_MDNS_NAME))
  {
    Serial.println("mDNS init failed -- frames will never find a destination.");
  }

  camera = new OV7670(CAMERA_MODE, SIOD, SIOC, VSYNC, HREF, XCLK, PCLK,
                      D0, D1, D2, D3, D4, D5, D6, D7);
  frameBytes = (size_t)camera->xres * camera->yres * 2;
  Serial.printf("Resolution %dx%d, %u bytes/frame\n",
                camera->xres, camera->yres, (unsigned)frameBytes);

  for (int i = 0; i < 2; i++)
  {
    slots[i] = (uint8_t *)malloc(frameBytes);
    if (!slots[i])
    {
      Serial.println("Out of memory allocating frame slots. Use QQVGA.");
      while (true)
        delay(1000);
    }
  }

  serverLock = xSemaphoreCreateMutex();
  freeQ = xQueueCreate(2, sizeof(uint8_t));
  readyQ = xQueueCreate(1, sizeof(FrameMsg));
  for (uint8_t i = 0; i < 2; i++)
    xQueueSend(freeQ, &i, 0);

  udp.begin(LOCAL_PORT);

  xTaskCreatePinnedToCore(captureTask, "capture", 4096, nullptr, 2, nullptr, 1);
  xTaskCreatePinnedToCore(senderTask, "sender", 4096, nullptr, 2, nullptr, 0);
  xTaskCreatePinnedToCore(discoveryTask, "discovery", 4096, nullptr, 1, nullptr, 0);
}

// --------------------------------------------------------------------- loop

void loop()
{
  static uint32_t lastMs = 0, lastSent = 0, lastCap = 0;
  const uint32_t now = millis();
  if (now - lastMs >= 1000)
  {
    bool known;
    xSemaphoreTake(serverLock, portMAX_DELAY);
    known = serverKnown;
    xSemaphoreGive(serverLock);

    Serial.printf("capture %lu fps | sent %lu fps | dropped %lu total | "
                  "server %s | heap %u\n",
                  (unsigned long)(framesCaptured - lastCap),
                  (unsigned long)(framesSent - lastSent),
                  (unsigned long)framesDropped,
                  known ? "known" : "searching...",
                  (unsigned)ESP.getFreeHeap());
    lastCap = framesCaptured;
    lastSent = framesSent;
    lastMs = now;
  }
  vTaskDelay(pdMS_TO_TICKS(100));
}
