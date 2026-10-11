import http from "k6/http";
import {
  SchemaRegistry,
} from "k6/x/kafka";

const schemaRegistry = new SchemaRegistry();

const tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds

export function TPSgenerate(tps, stepTm, acceleration, duration, type, options) {
  let [tpsStart, tpsEnd] = tps.split('.').map(Number);
  let step = parseInt(stepTm);
  let time1 = acceleration.split('.')[0];
  let time2 = acceleration.split('.')[1];
  let time3 = duration;
  let tpsTm = Math.ceil((tpsEnd - tpsStart) / step);
  
  const scenarios = Object.keys(options.scenarios);
  
  const stages = scenarios.map(() => ({
    tar: [],
    dur: []
  }));
  
  if (tpsStart === 0) {
    for (let i = 0; i < step; i++) {
      scenarios.forEach((_scenario, index) => {
        stages[index].tar.push(Math.ceil((tpsTm + i * tpsTm) * type[index]));
        stages[index].tar.push(Math.ceil((tpsTm + i * tpsTm) * type[index]));
        stages[index].dur.push(time2);
        stages[index].dur.push(time3);
      });
    }
  } else {
    scenarios.forEach((_scenario, index) => {
      stages[index].tar.push(Math.ceil(tpsStart * type[index]));
      stages[index].tar.push(Math.ceil(tpsStart * type[index]));
      stages[index].dur.push(time1);
      stages[index].dur.push(time3);
    });
  
    for (let i = 1; i < step + 1; i++) {
      scenarios.forEach((_scenario, index) => {
        stages[index].tar.push(Math.ceil((tpsStart + i * tpsTm) * type[index]));
        stages[index].tar.push(Math.ceil((tpsStart + i * tpsTm) * type[index]));
        stages[index].dur.push(time2);
        stages[index].dur.push(time3);
      });
    }
  }
  
  scenarios.forEach((scenario, index) => {
    options.scenarios[scenario].stages = stages[index].tar.map((target, idx) => ({
      target,
      duration: stages[index].dur[idx]
    }));
  });
  
  scenarios.forEach((scenario, index) => {
    options.scenarios[scenario].startRate = Math.ceil(options.scenarios[scenario].stages[0].target / 10);
    options.scenarios[scenario].preAllocatedVUs = Math.ceil((tpsEnd / 2) * type[index]);
  });
  
  return options;
}

export function randomString(length, charset='abcdefghijklmnopqrstuvwxyz') {
  let res = '';
  while (length--) res += charset[(Math.random() * charset.length) | 0];
  return res;
}

export function randomIntBetween(min, max) { // min and max included
  return Math.floor(Math.random() * (max - min + 1) + min);
}

export function CreDt(date){
  return date.toISOString().replace(/Z/g, "\+03\:00");
}

export function randomFF() {
  return Math.random() < 0.5 ? 1 : 2;
}

export function randomNum() {
  return Math.floor(Math.random() * 3) + 1;
}

export function subtractMonths(date, months) {
  return date.setMonth(date.getMonth() - months);
}

export function RqUid(){
  return crypto.randomUUID().replace(/-/g, "");
}

export function Plus100Ms(date){
  return date.setMilliseconds(date.getMilliseconds() + 100);
}

export function StepTm(date){
  return date.toISOString().replace(/Z/g, "");
}

export function randomNumber(length){
  let result = '';
  const characters = '12345678';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++){
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

export function Time(mode, offset){
  switch (mode) {
    case 'TimeTm': 
      return new Date(Date.now() - offset);
    case "CreDt": 
      return (new Date(Date.now() - offset)).toISOString().replace(/Z/g, "\+03\:00");
    case 'OperDt':
      return (new Date(Date.now() - offset)).toISOString().replace(/\.\d*Z/g, "");
    case 'DateTime':
      return (new Date(Date.now() - offset)).toISOString().replace(/\D/g, "");
    case 'RqTm':
      return (new Date(Date.now() - offset)).toISOString().replace(/Z/g, "");
    case 'RqTm2': 
      return (new Date(Date.now() - offset)).toISOString();
    case 'OperDate': 
      return (new Date(Date.now() - offset)).toISOString().replace(/T.*/g, "");
    case 'Summary': 
      return (new Date(Date.now() - offset)).toISOString().replace(/\D/g, "");
    case 'transfer': 
      return (new Date(Date.now() - offset)).toISOString().replace(/\.\d*Z/g, "\+03\:00");
    case 'JunOperDt': 
      let date = new Date(Date.now() - offset);
      date.setFullYear(2024);
      date.setMonth(0);
      return date.toISOString().replace(/\.\d*Z/g, "");
    default: 
      console.log(`Нет режима ${mode}.`);
  }
}

export function TimeRandDay(mode, offset, randomDay){
  switch (mode) {
    case 'RandomDay': 
      let date = new Date(Date.now() - offset);
      date.setFullYear(2025);
      date.setMonth(11);
      date.setDate(randomDay);
      return date.toISOString().replace(/\.\d*Z/g, "");
    default: 
      console.log(`Нет режима ${mode}.`);
  }
}

export function RefundsC2CRequest(body, ip){
  let res = http.post('https://test/test', body, {
    headers: {
      "content-type": "application/json",
      "SubsystemCode": "PPRBST"
  },}, {tags: { server: ip },});
  return res;
}
