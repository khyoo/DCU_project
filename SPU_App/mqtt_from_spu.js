// mqtt 모듈 가져오기
const mqtt = require('mqtt');

// configure 정보 가져오기
const CONFIG = require('./conf.json');
// spu 메시지 가져오기
let spu0_msg = require('./spu0_msg.json');
let spu1_msg = require('./spu1_msg.json');
let spu2_msg = require('./spu2_msg.json');
let spu3_msg = require('./spu3_msg.json');

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
            console.info(CONFIG.spu_id[0] + " : MQTT Client Connection Success!");
        }
    });

    let nowTime = "";

    // 3(3000)초 단위로 spu 메시지 발행
    setInterval(function () {
        nowTime = Math.floor(new Date().getTime() / 1000);
        spu0_msg.spu_time = nowTime;
        //console.log(nowTime);

        for (var i = 0; i < 5; i++) {
            spu0_msg.boxes[i].time = nowTime;

            var sensors_len = spu0_msg.boxes[i].sensors.length;
            for (var j = 0; j < sensors_len; j++) {
                spu0_msg.boxes[i].sensors[j].dt = nowTime;
            }
        }

        //console.log(spu0_msg);
        mqtt_client.publish(CONFIG.mqtt.topic[0], JSON.stringify(spu0_msg));
    }, CONFIG.spu_period);

    setTimeout(function() {
        // 3(3000)초 단위로 spu 메시지 발행
        setInterval(function () {
            nowTime = Math.floor(new Date().getTime() / 1000);
            spu1_msg.spu_time = nowTime;
            //console.log(nowTime);

            for (var i = 0; i < 5; i++) {
                spu1_msg.boxes[i].time = nowTime;

                var sensors_len = spu1_msg.boxes[i].sensors.length;
                for (var j = 0; j < sensors_len; j++) {
                    spu1_msg.boxes[i].sensors[j].dt = nowTime;
                }
            }

            //console.log(spu1_msg);
            mqtt_client.publish(CONFIG.mqtt.topic[0], JSON.stringify(spu1_msg));
        }, CONFIG.spu_period);
    }, 1000);

    setTimeout(function() {
        // 3(3000)초 단위로 spu 메시지 발행
        setInterval(function () {
            nowTime = Math.floor(new Date().getTime() / 1000);
            spu2_msg.spu_time = nowTime;
            //console.log(nowTime);

            for (var i = 0; i < 5; i++) {
                spu2_msg.boxes[i].time = nowTime;

                var sensors_len = spu2_msg.boxes[i].sensors.length;
                for (var j = 0; j < sensors_len; j++) {
                    spu2_msg.boxes[i].sensors[j].dt = nowTime;
                }
            }

            //console.log(spu2_msg);
            mqtt_client.publish(CONFIG.mqtt.topic[0], JSON.stringify(spu2_msg));
        }, CONFIG.spu_period);
    }, 2000);

    setTimeout(function() {
        // 3(3000)초 단위로 spu 메시지 발행
        setInterval(function () {
            nowTime = Math.floor(new Date().getTime() / 1000);
            spu3_msg.spu_time = nowTime;
            //console.log(nowTime);

            for (var i = 0; i < 5; i++) {
                spu3_msg.boxes[i].time = nowTime;

                var sensors_len = spu3_msg.boxes[i].sensors.length;
                for (var j = 0; j < sensors_len; j++) {
                    spu3_msg.boxes[i].sensors[j].dt = nowTime;
                }
            }

            //console.log(spu3_msg);
            mqtt_client.publish(CONFIG.mqtt.topic[0], JSON.stringify(spu3_msg));
        }, CONFIG.spu_period);
    }, 3000);
});

// mqtt message 이벤트 핸들러
mqtt_client.on('message', function (topic, message) {
    // message is Buffer
    console.log(message.toString())    
})