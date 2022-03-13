// mqtt 모듈 가져오기
const mqtt = require('mqtt');

// configure 정보 가져오기
const CONFIG = require('./conf.json');
// spu 메시지 가져오기
let spu_msg = require('./spu_msg.json');

const mqtt_options = {
    host: CONFIG.mqtt.host,
    port: CONFIG.mqtt.port,
    protocol: CONFIG.mqtt.protocol,
    username: CONFIG.mqtt.username,
    password: CONFIG.mqtt.password
};
// mqtt(SPU) connect
const mqtt_client = mqtt.connect(mqtt_options);

// mqtt connect 이벤트 핸들러
mqtt_client.on('connect', function () {
    mqtt_client.subscribe(CONFIG.mqtt.topic[0], function (err) {
        if (!err) {
            console.info(CONFIG.spu_id + " : MQTT Client Connection Success!");
        }
    });

    let nowTime = "";

    // 3(3000)초 단위로 spu 메시지 발행
    setInterval(function () {
        nowTime = Math.floor(new Date().getTime() / 1000);
        //console.log(nowTime);

        for (var i = 0; i < 5; i++) {
            spu_msg.boxes[i].time = nowTime;

            var sensors_len = spu_msg.boxes[i].sensors.length;
            for (var j = 0; j < sensors_len; j++) {
                spu_msg.boxes[i].sensors[j].dt = nowTime;
            }
        }

        //console.log(spu_msg);
        mqtt_client.publish(CONFIG.mqtt.topic[0], JSON.stringify(spu_msg));
    }, 3000);
});

// mqtt message 이벤트 핸들러
mqtt_client.on('message', function (topic, message) {
    // message is Buffer
    console.log(message.toString())    
})