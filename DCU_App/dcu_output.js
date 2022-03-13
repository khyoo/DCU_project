// mongoose 모듈 가져오기
const mongoose = require('mongoose');
// kafka-node 모듈 가져오기
const kafka = require('kafka-node');

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

// 현재 시간 UTC 변환
let nowTime = Math.floor(new Date().getTime() / 1000);

let header_msg = {
    "dcu_id": CONFIG.dcu_id,
    "dcu_time": nowTime,
    "dcu_payload": []
};

// kafka URI 정보
let kafkaURI = CONFIG.kafka.host + ':' + CONFIG.kafka.port;

// kafka Producer 객체 생성
let Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    kafkaClient = new kafka.KafkaClient({ kafkaHost: kafkaURI }),
    producer = new Producer(kafkaClient),
    km = new KeyedMessage('key', 'message'),
    payloads = [];
