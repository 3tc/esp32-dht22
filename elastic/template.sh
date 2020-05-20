curl -u elastic:changeme -X PUT "localhost:9200/_template/sensor-template" -H 'Content-Type: application/json' -d'
{
    "order": 0,
    "template": "sensor-*",
    "settings": {},
    "mappings": {
        "_default_": {
            "_all": {
                "enabled": false
            },
            "properties": {
                "timestamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "sensorData": {
                    "type": "float"
                },
                "topic": {
                    "type": "text"
                }
            }
        }
    },
    "aliases": {
        "sensor": {}
    }
}
'
