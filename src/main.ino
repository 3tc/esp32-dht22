#include <WiFiClientSecure.h>
#include <dht.h>
#include <PubSubClient.h>

#define STATUS_LED 2
#define DHT22_PIN 23

/* build flags
#define WLAN_SSID = "";
#define WLAN_PASS = "";

#define MQTT_HOST  = "localhost";
#define MQTT_PORT   = 8883;
#define MQTT_USER  = "";
#define MQTT_PASS  = "";
#define MQTT_TOPIC = "myTopic";
*/

/* /etc/ssl/certs/DST_Root_CA_X3.pem */
const char* CA_CERT = \
"-----BEGIN CERTIFICATE-----\n" \
"MIIDSjCCAjKgAwIBAgIQRK+wgNajJ7qJMDmGLvhAazANBgkqhkiG9w0BAQUFADA/\n" \
"MSQwIgYDVQQKExtEaWdpdGFsIFNpZ25hdHVyZSBUcnVzdCBDby4xFzAVBgNVBAMT\n" \
"DkRTVCBSb290IENBIFgzMB4XDTAwMDkzMDIxMTIxOVoXDTIxMDkzMDE0MDExNVow\n" \
"PzEkMCIGA1UEChMbRGlnaXRhbCBTaWduYXR1cmUgVHJ1c3QgQ28uMRcwFQYDVQQD\n" \
"Ew5EU1QgUm9vdCBDQSBYMzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\n" \
"AN+v6ZdQCINXtMxiZfaQguzH0yxrMMpb7NnDfcdAwRgUi+DoM3ZJKuM/IUmTrE4O\n" \
"rz5Iy2Xu/NMhD2XSKtkyj4zl93ewEnu1lcCJo6m67XMuegwGMoOifooUMM0RoOEq\n" \
"OLl5CjH9UL2AZd+3UWODyOKIYepLYYHsUmu5ouJLGiifSKOeDNoJjj4XLh7dIN9b\n" \
"xiqKqy69cK3FCxolkHRyxXtqqzTWMIn/5WgTe1QLyNau7Fqckh49ZLOMxt+/yUFw\n" \
"7BZy1SbsOFU5Q9D8/RhcQPGX69Wam40dutolucbY38EVAjqr2m7xPi71XAicPNaD\n" \
"aeQQmxkqtilX4+U9m5/wAl0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNV\n" \
"HQ8BAf8EBAMCAQYwHQYDVR0OBBYEFMSnsaR7LHH62+FLkHX/xBVghYkQMA0GCSqG\n" \
"SIb3DQEBBQUAA4IBAQCjGiybFwBcqR7uKGY3Or+Dxz9LwwmglSBd49lZRNI+DT69\n" \
"ikugdB/OEIKcdBodfpga3csTS7MgROSR6cz8faXbauX+5v3gTt23ADq1cEmv8uXr\n" \
"AvHRAosZy5Q6XkjEGB5YGV8eAlrwDPGxrancWYaLbumR9YbK+rlmM6pZW87ipxZz\n" \
"R8srzJmwN0jP41ZL9c8PDHIyh8bwRLtTcm1D9SZImlJnt1ir/md2cXjbDaJWFBM5\n" \
"JDGFoqgCWjBH4d1QB7wCCZAA62RjYJsWvIjJEubSfZGL+T0yjWW06XyxV3bqxbYo\n" \
"Ob8VZRzI9neWagqNdwvYkQsEjgfbKbYK7p2CNTUQ\n" \
"-----END CERTIFICATE-----\n"; 

WiFiClientSecure espClient;
PubSubClient mqttClient(espClient);
dht DHT;

void setup() {
  Serial.begin(115200);
  pinMode(STATUS_LED, OUTPUT);
  digitalWrite(STATUS_LED, LOW);
  connectWifi();
  delay(10);
  espClient.setCACert(CA_CERT);
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }
  while (DHT.read22(DHT22_PIN) != 0) {
    Serial.println("DHT22 read failed, retrying...");
    delay(500);
  }
  char temperature[10];
  char humidity[10];
  publish("temp", dtostrf(DHT.temperature, 0, 2, temperature));
  publish("humidity", dtostrf(DHT.humidity, 0, 2, humidity));
  delay(500);
  mqttClient.disconnect();
  
  delay(30000);
}

void connectWifi() {
  Serial.println();
  Serial.print("Connecting to ");
  Serial.println(WLAN_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WLAN_SSID, WLAN_PASS);
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("WiFi connected");  
  Serial.println("IP address: ");
  Serial.println(WiFi.localIP());
}

void publish(const char* topic, const char* message) {
    digitalWrite(STATUS_LED, true);
    if (!mqttClient.connected()) {
        reconnect();
    }
    char mqttTopic[80];
    sprintf(mqttTopic, "%s/%s", MQTT_TOPIC, topic); 
    Serial.print("publishing [");
    Serial.print(message);
    Serial.print("] to topic [");
    Serial.print(mqttTopic);
    Serial.println("]");
    mqttClient.publish(mqttTopic, message);
    digitalWrite(STATUS_LED, false);
}

void reconnect() {
  while (!mqttClient.connected()) {
    Serial.print("Attempting MQTT connection...");
    // Attempt to connect
    if (mqttClient.connect("esp32", MQTT_USER, MQTT_PASS)) {
      Serial.println("connected");
    } else {
      Serial.print("failed, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

/*
void smartConfigWiFi() {
    WiFi.mode(WIFI_AP_STA);
    WiFi.beginSmartConfig();

    // wait for smartconfig packet from mobile app
    Serial.println("Waiting for SmartConfig.");
    while (!WiFi.smartConfigDone()) {
        delay(500);
        Serial.print(".");
        digitalWrite(STATUS_LED, !digitalRead(STATUS_LED));
    }
    Serial.println("");
    Serial.println("SmartConfig received");

    // wait to connect to AP
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
        digitalWrite(STATUS_LED, !digitalRead(STATUS_LED));
    }
    digitalWrite(STATUS_LED, LOW);
    Serial.print("WiFi connected to ");  
    Serial.println(WiFi.SSID());  
    Serial.print("IP address: ");
    Serial.println(WiFi.localIP());
    Serial.print("ESP Mac Address: ");
    Serial.println(WiFi.macAddress());
    Serial.print("Subnet Mask: ");
    Serial.println(WiFi.subnetMask());
    Serial.print("Gateway IP: ");
    Serial.println(WiFi.gatewayIP());
    Serial.print("DNS: ");
    Serial.println(WiFi.dnsIP());
}
*/
