// mqtt 모듈 가져오기
const mqtt = require('mqtt')
// mongoose 모듈 가져오기
const mongoose = require('mongoose');

// configure 정보 가져오기
const CONFIG = require('./conf.json');

// mongoDB URI 정보
let mongoURI = 'mongodb://' + CONFIG.mongodb.host + ':' + CONFIG.mongodb.port + '/' + CONFIG.mongodb.db;
// mongoDB connect
mongoose.connect(mongoURI);

// mongoDB 연결 성공 이벤트 핸들러
mongoose.connection.once('open', function () {
    console.info(CONFIG.dcu_id + ' : MongoDB Connection Success!');
});

// mongoDB 연결 실패 이벤트 핸들러
mongoose.connection.on('error', function () {
    console.error(CONFIG.dcu_id + ' : MongoDB Connection Failed!');
});

// SPU message schema 객체 생성
let spu_schema = mongoose.Schema({
    spu_id: String,
    boxes: Object
});

// 정의된 스키마를 객체처럼 사용할 수 있도록 model() 함수로 컴파일
let SPU_Schema = mongoose.model('Sensor', spu_schema);

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
            console.info(CONFIG.dcu_id + " : MQTT Client Connection Success!");
        }
    });
});

// mqtt message 이벤트 핸들러
mqtt_client.on('message', function (topic, message) {
    // message is Buffer    
    let msg_parse = JSON.parse(message);
    let spu_messages = new SPU_Schema(msg_parse);

    spu_messages.save(function (error, data) {
        if (error)
            console.error(error);        
    });    
})



