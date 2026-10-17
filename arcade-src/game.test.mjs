import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {FlightModel,terrain,circleRect,HelicopterGame} from './game.mjs';
test('hold rises; release falls; walls end the flight',()=>{
 const up=new FlightModel(()=>.5),down=new FlightModel(()=>.5);for(let i=0;i<60;i++){up.update(1/120,true);down.update(1/120,false);}assert.ok(up.y<260);assert.ok(down.y>260);
 for(let i=0;i<1000&&!down.dead;i++)down.update(1/120,false);assert.ok(down.dead);
});
test('obstacles collide and fixed step conserves distance across frame rates',()=>{
 const model=new FlightModel();model.obstacles=[{x:180,y:250,w:35,h:80}];model.update(1/120,false);assert.ok(model.dead);
 assert.ok(circleRect(10,10,3,{x:9,y:9,w:10,h:10}));assert.ok(!circleRect(0,0,3,{x:9,y:9,w:10,h:10}));
 const a=new FlightModel(),b=new FlightModel();a.nextObstacle=b.nextObstacle=1e9;
 for(let i=0;i<60;i++)a.update(1/60,i>30);for(let i=0;i<120;i++)b.update(1/120,i>60);assert.equal(a.distance,b.distance);
 for(let x=0;x<20000;x+=100){const t=terrain(x,5000);assert.ok(t.bottom-t.top>250);}
});
test('renderer, state transitions, pause, restart and persistence run',()=>{
 globalThis.requestAnimationFrame=()=>1;globalThis.localStorage={getItem:()=> '24',setItem:()=>{}};
 const context=new Proxy({},{get:(obj,key)=>obj[key]??(()=>{}),set:(obj,key,value)=>(obj[key]=value,true)});
 const game=new HelicopterGame({getContext:()=>context});assert.equal(game.best,24);game.start();game.held=true;
 for(let i=0;i<20;i++)game.frame(1000+i*16.67);game.pause();const travel=game.model.travel;game.frame(2000);assert.equal(game.model.travel,travel);game.resume();game.frame(2017);assert.ok(game.model.travel>travel);game.crash();assert.equal(game.state,'over');game.start();assert.equal(game.model.distance,0);
});
test('both standalone pages contain parseable scripts and no missing referenced IDs',async()=>{
 for(const path of ['../arcade.html','../games/helicopter.html']){
  const html=await readFile(new URL(path,import.meta.url),'utf8');const scripts=[...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
  for(const [,script]of scripts)new vm.Script(script);
  const ids=[...html.matchAll(/\bid="([^"]+)"/g)].map(x=>x[1]);assert.equal(ids.length,new Set(ids).size,'duplicate IDs');
  for(const [,id]of html.matchAll(/\$\('([^']+)'\)/g))assert.ok(ids.includes(id),`missing ${id}`);
  assert.ok(!html.includes('__GAME_SOURCE__'));
 }
});
