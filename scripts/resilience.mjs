#!/usr/bin/env node
// Intentionally stops/restarts ONLY this repository's Compose services.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const cwd=fileURLToPath(new URL('../',import.meta.url));
const args=process.argv.slice(2);
if(args.some(arg=>arg!=='--poison-only')) throw Error('Usage: node scripts/resilience.mjs [--poison-only]');
const token=process.env.DEMO_TOKEN ?? 'local-demo-token';
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function docker(args,input) {
 return execFileSync('docker',['compose',...args],{cwd,input,encoding:'utf8',timeout:120000,stdio:['pipe','pipe','pipe']});
}
async function api(port,path,body,extra={}) {
 const r=await fetch('http://localhost:'+port+path,{
  method:body===undefined ? 'GET':'POST',
  headers:{'Content-Type':'application/json','X-Demo-Token':token,...extra},
  body:body===undefined ? undefined:JSON.stringify(body),signal:AbortSignal.timeout(7000)});
 const text=await r.text();
 assert.ok(r.ok, `${path} returned ${r.status}: ${text}`);
 return text ? JSON.parse(text):null;
}
async function until(label,probe,ok,ms=110000) {
 const end=Date.now()+ms; let last;
 while(Date.now()<end) {
  try { last=await probe(); if(ok(last)) return last; } catch(e) { last=e.message; }
  await sleep(250);
 }
 throw Error(label+' timed out; last='+JSON.stringify(last));
}
async function order(mode) {
 return api(8080,'/orders',{mode,sku:'SKU-1',quantity:1,amountCents:1200,fault:'NONE'},
  {'Idempotency-Key':'resilience-'+randomUUID()});
}
async function verify(id,status,before) {
 await until('participant convergence',async()=>Promise.all([
  api(18081,'/inventory/'+id),api(8082,'/payments/'+id),api(8083,'/shipments/'+id)]),
  rows=>rows.map(r=>r.state).join(',')===(status==='COMPLETED' ? 'RESERVED,CHARGED,BOOKED':'RELEASED,REFUNDED,CANCELLED'));
 assert.equal((await api(18081,'/inventory/stock/SKU-1')).available,before-(status==='COMPLETED'?1:0));
}
for(const mode of args.includes('--poison-only') ? [] : ['ORCHESTRATION','CHOREOGRAPHY']) {
 console.log(mode+' broker outage: starting');
 const before=(await api(18081,'/inventory/stock/SKU-1')).available;
 let accepted;
 try {
  docker(['stop','kafka']);
  accepted=await order(mode);
  assert.ok((await api(8080,'/ops/outbox')).pending>0,'Committed request must retain its outbox');
 } finally { docker(['start','kafka']); }
 const settled=await until('broker recovery',()=>api(8080,'/orders/'+accepted.id),
  o=>['COMPLETED','CANCELLED'].includes(o.status));
 await verify(accepted.id,settled.status,before);
 console.log(mode+' broker outage: PASS '+settled.status+' '+accepted.id);

 console.log(mode+' restart and durable timeout: starting');
 const stock=(await api(18081,'/inventory/stock/SKU-1')).available;
 let pending;
 try {
  docker(['stop','payment-service']);
  pending=await order(mode);
  await until('reservation',()=>api(18081,'/inventory/'+pending.id),r=>r.state==='RESERVED');
  docker(['restart','order-service']);
  await until('persisted deadline after restart',()=>api(8080,'/orders/'+pending.id),
   o=>['CANCELLING_SHIPPING','REFUNDING','RELEASING','COMPENSATING','MANUAL_INTERVENTION'].includes(o.status));
 } finally { docker(['start','payment-service']); }
 let cancelled=await until('restart recovery',()=>api(8080,'/orders/'+pending.id),
  o=>['CANCELLED','MANUAL_INTERVENTION'].includes(o.status));
 if(cancelled.status==='MANUAL_INTERVENTION') {
  await api(8080,'/orders/'+pending.id+'/retry',{});
  cancelled=await until('manual recovery',()=>api(8080,'/orders/'+pending.id),o=>o.status==='CANCELLED');
 }
 await verify(pending.id,cancelled.status,stock);
 console.log(mode+' restart and durable timeout: PASS '+pending.id);
}
const sagaId=randomUUID(), poisonId=randomUUID();
const poison={version:99,id:poisonId,sagaId,mode:'ORCHESTRATION',type:'ORDER_CREATED',
 sku:'SKU-1',quantity:1,amountCents:1200,fault:'NONE'};
docker(['exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh',
 '--bootstrap-server','kafka:19092','--topic','ecommerce.saga',
 '--property','parse.key=true','--property','key.separator=|'],sagaId+'|'+JSON.stringify(poison)+'\n');
for(const port of [8080,18081,8082,8083]) {
 await until('poison quarantine '+port,()=>api(port,'/ops/failures'),
  rows=>rows.some(r=>r.payload.includes(poisonId)));
}
let dlt;
try {
 dlt=docker(['exec','-T','kafka','/opt/kafka/bin/kafka-console-consumer.sh',
  '--bootstrap-server','kafka:19092','--topic','order-service.DLT','--from-beginning','--timeout-ms','10000']);
} catch(error) {
 // The console consumer exits 1 at its idle timeout; inspect all retained records.
 // Never assume the first record belongs to this run.
 if(error.status!==1) throw error;
 dlt=error.stdout ?? '';
}
assert.ok(dlt.includes(poisonId),'Order DLT must contain the exact poison record');
console.log('Poison quarantine and actual Kafka DLT: PASS '+poisonId);
console.log('All resilience exercises passed.');
