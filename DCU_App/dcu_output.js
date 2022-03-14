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
    spu_time: Number,
    boxes: Object
});

// 정의된 스키마를 객체처럼 사용할 수 있도록 model() 함수로 컴파일
let SPU_Schema = mongoose.model('Sensor', spu_schema);

// 현재 시간 UTC 변환
let nowTime = Math.floor(new Date().getTime() / 1000);

let aggr_msg = {
    "dcu_id": CONFIG.dcu_id,
    "dcu_time": 0,
    "dcu_payload": []
};

// kafka URI 정보
let kafkaURI = CONFIG.kafka.host + ':' + CONFIG.kafka.port;

// kafka Producer 객체 생성
let Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    kafkaClient = new kafka.KafkaClient({ kafkaHost: kafkaURI }),
    producer = new Producer(kafkaClient),
    km = new KeyedMessage('key', 'message');

let payloads_dcu = [];
let spu0_temp = {};
let spu1_temp = {};
let spu2_temp = {};
let spu3_temp = {};

var interval = setInterval(function() {
    nowTime = Math.floor(new Date().getTime() / 1000);
    if ((nowTime % 10) == 0) {      
        clearInterval(interval);

        setInterval(function () {
            nowTime = Math.floor(new Date().getTime() / 1000);
            aggr_msg.dcu_time = nowTime;

            SPU_Schema.findOne({'spu_id':'spu0', 'spu_time': {'$gte':nowTime-(CONFIG.aggr_period/1000), '$lte':nowTime}}, null, {sort: {'spu_time':-1}}, function(err, data){
                if(err) {
            
                } else {
                    spu0_temp = data;
                    //console.log(data);
                }
            });
            SPU_Schema.findOne({'spu_id':'spu1', 'spu_time': {'$gte':nowTime-(CONFIG.aggr_period/1000), '$lte':nowTime}}, null, {sort: {'spu_time':-1}}, function(err, data){
                if(err) {
            
                } else {
                    spu1_temp = data;
                    //console.log(data);
                }
            });
            SPU_Schema.findOne({'spu_id':'spu2', 'spu_time': {'$gte':nowTime-(CONFIG.aggr_period/1000), '$lte':nowTime}}, null, {sort: {'spu_time':-1}}, function(err, data){
                if(err) {
            
                } else {
                    spu2_temp = data;
                    //console.log(data);
                }
            });
            SPU_Schema.findOne({'spu_id':'spu3', 'spu_time': {'$gte':nowTime-(CONFIG.aggr_period/1000), '$lte':nowTime}}, null, {sort: {'spu_time':-1}}, function(err, data){
                if(err) {
            
                } else {
                    spu3_temp = data;
                    //console.log(data);
                }
            });

            aggr_msg.dcu_payload.push(spu0_temp);
            aggr_msg.dcu_payload.push(spu1_temp);
            aggr_msg.dcu_payload.push(spu2_temp);
            aggr_msg.dcu_payload.push(spu3_temp);
        
            payloads_dcu = [{
                topic: CONFIG.kafka.topic[0],
                messages: JSON.stringify(aggr_msg),
                partition: 0
            }];
            
            producer.send(payloads_dcu, function (err, data) {
                //console.log(data);    
                aggr_msg.dcu_payload.length = 0;
            });
        },  CONFIG.aggr_period);
    }
  }, 1000);



