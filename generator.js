import {
  Writer,
  Connection,
  TLS_1_2,
  BALANCER_MURMUR2,
} from "k6/x/kafka";
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';
import * as utils from '/home/test/Genk6/utils/utils.js';
import * as messages from './messages.js';

import redis from 'k6/x/redis';

const Type = [0, 0.425, 0.05, 0, 0.0426, 0.0053, 0.001];

const tzoffset = -10800000; //offset +3 hour
const oldoffset = 2667600000; //offset -1 month

const C2C = new Counter('C2C');
const C2CError = new Counter('C2CError');

const optionsTM = {
  discardResponseBodies: true,
  tlsAuth: [
    {
      domains: [],
      cert: open('../../certC2C.pem'),
      key: open('../../keyC2C.pem'),
    },
  ],
  scenarios: {
    PrepC2C: {
      executor: "ramping-arrival-rate",
      exec: 'PrepC2C',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    ExecC2C: {
      executor: "ramping-arrival-rate",
      exec: 'ExecC2C',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    RollbackC2C: {
      executor: "ramping-arrival-rate",
      exec: 'RollbackC2C',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    PrepPM: {
      executor: "ramping-arrival-rate",
      exec: 'PrepPM',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    ExecPM: {
      executor: "ramping-arrival-rate",
      exec: 'ExecPM',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    RollbackPM: {
      executor: "ramping-arrival-rate",
      exec: 'RollbackPM',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
    RefundsC2C: {
      executor: "ramping-arrival-rate",
      exec: 'RefundsC2C',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 0,
      stages: [],
    },
  },
};

export const options = Object.assign(
  {},
  utils.TPSgenerate(__ENV.MY_TPS, __ENV.MY_STEP, __ENV.MY_ACCELERATION, __ENV.MY_DURATION, Type, optionsTM),
  { noConnectionReuse: true }
);

const brokers = ["1:9093","2:9093","3:9093","4:9093"];
const topicWriteTransfer = "test";

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
  minVersion: TLS_1_2,
  clientCertPem: "kafkacert.pem",
  clientKeyPem: "kafkakey.pem",
  serverCaPem: "serverca.pem",
};
const writerTransfer = new Writer({
  brokers: brokers,
  topic: topicWriteTransfer,
  tls: tlsConfig,
  batchBytes: 10485760,
  balancer: BALANCER_MURMUR2,
  acks: "all",
});
const connection = new Connection({
  address: brokers[0],
  tls: tlsConfig,
});

const redisCounter = new redis.Client(
  "redis://12:6379/0"
);
const redisWrite = new redis.Client(
  "redis://13:6379/6"
);
const redisRead = new redis.Client(
  "redis://14:6379/7"
);
const redisRefTrue = new redis.Client(
  "redis://15:6379/8"
);

const message = new SharedArray('message', function () {
  let f = new Array(1);
  f[0] = JSON.parse(open('RefundsC2C.json'));
  return f; 
});

export function PrepC2C() {
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqTm,
      rqOperUid0,
      stepTm,
      message,
      date;

  
  rqUid = utils.RqUid();
  operDt = utils.Time('OperDt', tzoffset);
  dateTime = utils.Time('DateTime', tzoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', tzoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', tzoffset);
  date = utils.Time('OperDt', tzoffset);
  
  message = messages.Prep(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Prep(C2C)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'PrepC2C'});
  redisCounter.incr('prep_c2c');
}

export function PrepPM() {
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqTm,
      rqOperUid0,
      stepTm,
      message,
      date;

  rqUid = utils.RqUid();
  operDt = utils.Time('JunOperDt', oldoffset);
  dateTime = utils.Time('DateTime', oldoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', oldoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', oldoffset);
  date = utils.Time('OperDt', tzoffset);
  
  message = messages.Prep(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Prep(PM)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'PrepPM'});
  redisCounter.incr('prep_pm');
}

export function ExecC2C(){
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqTm,
      rqOperUid0,
      stepTm,
      message,
      date;

  
  rqUid = utils.RqUid();
  operDt = utils.Time('OperDt', tzoffset);
  dateTime = utils.Time('DateTime', tzoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', tzoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', tzoffset);
  date = utils.Time('OperDt', tzoffset);

  message = messages.Exec(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Exec(C2C)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'ExecC2C'});
  redisCounter.incr('exec_c2c');
}

export function ExecPM(){
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqTm,
      rqOperUid0,
      stepTm,
      message,
      date;

  
  rqUid = utils.RqUid();
  operDt = utils.Time('JunOperDt', oldoffset);
  dateTime = utils.Time('DateTime', oldoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', oldoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', oldoffset);
  date = utils.Time('OperDt', tzoffset);

  message = messages.Exec(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Exec(PM)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'ExecPM'});
  redisCounter.incr('exec_pm');
}

export function RollbackC2C() {
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqOperUid0,
      rqTm,
      stepTm,
      message,
      randomNum,
      date;

  
  rqUid = utils.RqUid();
  operDt = utils.Time('OperDt', tzoffset);
  dateTime = utils.Time('DateTime', tzoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', tzoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', tzoffset);
  date = utils.Time('OperDt', tzoffset);

  message = messages.ExecRollback(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Exec(C2C)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'ExecC2C'});
  redisCounter.incr('exec_c2c');

  
  randomNum = utils.randomString(19, `0123456789`);

  message = messages.Rollback(rqUid, operDt, dateTime, operUid0, rqTm, stepTm, randomNum, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Rollback(C2C)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'RollbackC2C'});
  redisCounter.incr('rollback_с2с');
}

export function RollbackPM() {
  let rqUid,
      operDt,
      dateTime,
      operUid0,
      rqOperUid0,
      rqTm,
      stepTm,
      message,
      randomNum,
      date;

  
  rqUid = utils.RqUid();
  operDt = utils.Time('JunOperDt', oldoffset);
  dateTime = utils.Time('DateTime', oldoffset);
  operUid0 = utils.RqUid();
  rqTm = utils.Time('RqTm', oldoffset);
  rqOperUid0 = utils.RqUid();
  stepTm = utils.Time('TimeTm', oldoffset);
  date = utils.Time('OperDt', tzoffset);
  message = messages.ExecRollback(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Exec(PM)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'ExecPM'});
  redisCounter.incr('exec_pm');

  
  randomNum = utils.randomString(19, `0123456789`);

  message = messages.Rollback(rqUid, operDt, dateTime, operUid0, rqTm, stepTm, randomNum, "C2C", date);
  writerTransfer.produce({ messages: message });
  //utils.SendMetrics(startTm, 'Rollback(PM)', __ENV.MY_IP, 'OK');
  C2C.add(1, {instance: __ENV.MY_IP, type: 'RollbackPM'});
  redisCounter.incr('rollback_pm');
}

export async function RefundsC2C() {
  let operUid,
      operDate,
      rqUid,
      rqTm,
      body,
      rollbackTF,
      dbquery,
      randomKey,
      obj,
      res;

  if (Math.random() < 0.7) {
    randomKey = await redisWrite.randomKey();
    dbquery = await redisWrite.getDel(randomKey);
    obj = JSON.parse(dbquery);
    operUid = obj.operUid;
    operDate = obj.operDt;
    rqUid = utils.randomString(32, 'abcdef0123456789ABCDEF');
    rqTm = utils.Time('RqTm2', tzoffset);
    rollbackTF = '"rollback": false';
    redisRefTrue.set(`${operUid}`, `{"operDt": "${operDate}","operUid": "${operUid}"}`);
    C2C.add(1, {instance: __ENV.MY_IP, type: 'RefundsC2C(False)'});
  } else {
    randomKey = await redisRefTrue.randomKey();
    dbquery = await redisRefTrue.getDel(randomKey);
    rollbackTF = '"rollback": true';

    if (randomKey == null) {
      randomKey = await redisRead.randomKey();
      dbquery = await redisRead.getDel(randomKey);
      rollbackTF = '"rollback": false';
    }

    obj = JSON.parse(dbquery);
    operUid = obj.operUid;
    operDate = obj.operDt;
    rqUid = utils.randomString(32, 'abcdef0123456789ABCDEF');
    rqTm = utils.Time('RqTm2', tzoffset);
    C2C.add(1, {instance: __ENV.MY_IP, type: 'RefundsC2C(True)'});
  }
  body = JSON.stringify(message[0])
    .replace("{{var_rqUid}}", rqUid)
    .replace("{{var_rqTm}}", rqTm)
    .replace("{{var_operUid}}", operUid)
    .replace("{{var_operDate}}", operDate)
    .replace(/"rollback":\s*false/, rollbackTF);
  
  res = await utils.RefundsC2CRequest(body, __ENV.MY_IP);

  if (res.status === 0 || res.status.code === 0 || res.status === 200) {
    //utils.SendMetrics(res.timings.duration, 'Detail', __ENV.MY_IP, 'OK');
  } else {
    C2CError.add(1, {instance: __ENV.MY_IP, type: 'RefundsC2C_Error', status: res.status}); 
  } 
  
/*   if (res.json().status === 0 || res.json().status.code === 0) {
    utils.SendMetrics(res.timings.duration, 'Detail', __ENV.MY_IP, 'OK');
  } else {
    utils.SendMetrics(res.timings.duration, 'Detail', __ENV.MY_IP, 
      (res.json().status !== undefined ? res.json().status : '') + " " + 
      (res.json().status.code !== undefined ? res.json().status.code : '') + " " + 
      (res.json().status.description)
    ); 
  } */
}

export function teardown(data) {
  writerTransfer.close();
  connection.close();
//  utils.EndSample();
}
