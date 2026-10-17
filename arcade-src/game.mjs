// Original canvas artwork and deterministic-time flight mechanics.
export const WIDTH = 960, HEIGHT = 540;
export function terrain(x, distance = 0) {
  const squeeze = Math.min(distance / 160, 30);
  return { top: Math.round((59 + Math.sin(x / 250) * 17 + Math.sin(x / 83) * 6 + squeeze) / 6) * 6,
    bottom: Math.round((465 + Math.sin(x / 290 + 1.3) * 22 + Math.sin(x / 105) * 9 - squeeze) / 6) * 6 };
}
export function circleRect(x,y,r,rect) {
  const dx=x-Math.max(rect.x,Math.min(x,rect.x+rect.w));
  const dy=y-Math.max(rect.y,Math.min(y,rect.y+rect.h));
  return dx*dx+dy*dy < r*r;
}
export class FlightModel {
  constructor(random=Math.random) { this.random=random; this.reset(); }
  reset() { this.x=180;this.y=260;this.vy=0;this.travel=0;this.distance=0;this.time=0;this.dead=false;this.obstacles=[];this.nextObstacle=1050; }
  update(dt,thrust) {
    if(this.dead)return;
    dt=Math.min(dt,1/60);this.time+=dt;
    this.vy=Math.max(-265,Math.min(285,this.vy+(thrust?-600:440)*dt));
    this.y+=this.vy*dt;this.travel+=(185+Math.min(this.distance/12,100))*dt;
    this.distance=Math.floor(this.travel/10);
    while(this.nextObstacle < this.travel+WIDTH+50) {
      const t=terrain(this.nextObstacle,this.distance), h=90+this.random()*36;
      const k=this.random();
      this.obstacles.push({x:this.nextObstacle,w:35,h,y:k<.4?t.top:k<.8?t.bottom-h:t.top+90+this.random()*70});
      this.nextObstacle+=370+this.random()*145;
    }
    this.obstacles=this.obstacles.filter(o=>o.x+o.w>this.travel-80);
    const t=terrain(this.x+this.travel,this.distance);
    if(this.y-13<t.top || this.y+13>t.bottom)this.dead=true;
    for(const o of this.obstacles)if(circleRect(this.x,this.y,17,{...o,x:o.x-this.travel}))this.dead=true;
  }
}
export class HelicopterGame {
  constructor(canvas,onChange=()=>{}) {
    this.canvas=canvas;this.ctx=canvas.getContext('2d');this.model=new FlightModel();this.onChange=onChange;
    this.state='ready';this.held=false;this.particles=[];this.last=0;this.acc=0;this.sound=false;this.audio=null;
    this.best=0;try{this.best=Number(localStorage.getItem('arcade-helicopter-best'))||0;}catch{}
    canvas.width=WIDTH;canvas.height=HEIGHT;
    this.frame=this.frame.bind(this);this.raf=requestAnimationFrame(this.frame);this.notify();
  }
  notify(){this.onChange({state:this.state,distance:this.model.distance,best:this.best});}
  start(){this.model.reset();this.particles=[];this.state='playing';this.acc=0;this.held=false;this.notify();}
  pause(){if(this.state==='playing'){this.state='paused';this.held=false;this.notify();}}
  resume(){if(this.state==='paused'){this.state='playing';this.acc=0;this.notify();}}
  crash(){this.state='over';this.held=false;this.best=Math.max(this.best,this.model.distance);try{localStorage.setItem('arcade-helicopter-best',String(this.best));}catch{}this.beep(80,.2);this.notify();}
  beep(freq,duration){if(!this.sound)return;try{this.audio??=new (window.AudioContext||window.webkitAudioContext)();this.audio.resume();const o=this.audio.createOscillator(),g=this.audio.createGain();o.type='triangle';o.frequency.value=freq;o.connect(g);g.connect(this.audio.destination);g.gain.setValueAtTime(.045,this.audio.currentTime);g.gain.exponentialRampToValueAtTime(.001,this.audio.currentTime+duration);o.start();o.stop(this.audio.currentTime+duration);}catch{}}
  frame(now){const dt=this.last?Math.min((now-this.last)/1000,.05):0;this.last=now;
    if(this.state==='playing'){
      this.acc+=dt;while(this.acc>=1/120){this.model.update(1/120,this.held);this.acc-=1/120;if(this.model.dead){this.crash();break;}}
      if(Math.random()<.8)this.particles.push({x:this.model.x-29,y:this.model.y+4,life:1});
      for(const p of this.particles){p.x-=dt*190;p.life-=dt*.85;p.y+=dt*9;}
      this.particles=this.particles.filter(p=>p.life>0);this.notify();
    }
    this.draw(now/1000);this.raf=requestAnimationFrame(this.frame);
  }
  draw(time){const c=this.ctx,m=this.model;c.fillStyle='#070b09';c.fillRect(0,0,WIDTH,HEIGHT);
    // Faint depth grid sits behind the intentionally simple neon cave.
    c.strokeStyle='#0c1b10';c.lineWidth=1;for(let x=-(m.travel*.25)%60;x<WIDTH;x+=60){c.beginPath();c.moveTo(x,0);c.lineTo(x,HEIGHT);c.stroke();}
    for(let y=0;y<HEIGHT;y+=60){c.beginPath();c.moveTo(0,y);c.lineTo(WIDTH,y);c.stroke();}
    c.fillStyle='#67f466';
    for(let x=0;x<WIDTH;x+=8){const t=terrain(x+m.travel,m.distance);c.fillRect(x,0,9,t.top);c.fillRect(x,t.bottom,9,HEIGHT-t.bottom);}
    for(const o of m.obstacles)c.fillRect(Math.round(o.x-m.travel),o.y,o.w,o.h);
    c.save();c.globalCompositeOperation='source-atop';c.fillStyle='rgba(0,20,0,.20)';
    // Horizontal engraving on the cave blocks, as in the reference.
    for(let x=0;x<WIDTH;x+=8){const t=terrain(x+m.travel,m.distance);for(let y=5;y<t.top;y+=8)c.fillRect(x,y,8,1);for(let y=Math.ceil(t.bottom/8)*8;y<HEIGHT;y+=8)c.fillRect(x,y,8,1);}
    for(const o of m.obstacles)for(let y=o.y+5;y<o.y+o.h;y+=8)c.fillRect(o.x-m.travel,y,o.w,1);
    c.restore();
    for(const p of this.particles){c.fillStyle=`rgba(165,184,175,${p.life*.45})`;c.beginPath();c.arc(p.x,p.y,3+(1-p.life)*4,0,Math.PI*2);c.fill();}
    c.save();c.translate(m.x,m.y);c.rotate(Math.max(-.14,Math.min(.19,m.vy/1400)));
    c.fillStyle='#7fa3bc';c.beginPath();c.moveTo(-19,0);c.lineTo(-43,-7);c.lineTo(-45,5);c.lineTo(-18,7);c.fill();
    c.fillStyle='#cce5f2';c.beginPath();c.ellipse(0,3,28,13,0,0,Math.PI*2);c.fill();
    c.fillStyle='#5ab4d9';c.beginPath();c.ellipse(13,-1,11,9,-.2,-1.5,1.5);c.fill();
    c.strokeStyle='#7595b2';c.lineWidth=3;c.beginPath();c.moveTo(-9,-4);c.lineTo(-9,12);c.moveTo(3,-6);c.lineTo(3,13);c.stroke();
    c.strokeStyle='#a4b8c4';c.lineWidth=2;c.beginPath();c.moveTo(-13,14);c.lineTo(-15,20);c.lineTo(20,20);c.lineTo(24,17);c.moveTo(8,14);c.lineTo(10,20);c.moveTo(-2,-10);c.lineTo(-2,-20);c.stroke();
    const rotor=(this.state==='playing'?Math.sin(time*75):.75);c.strokeStyle='#70a7cf';c.lineWidth=3;c.beginPath();c.moveTo(-39*rotor,-20);c.lineTo(39*rotor,-20);c.stroke();
    c.restore();c.fillStyle='rgba(2,5,3,.10)';for(let y=0;y<HEIGHT;y+=4)c.fillRect(0,y,WIDTH,1);
    if(this.state==='over'){c.strokeStyle='#ffad79';c.lineWidth=3;for(let i=0;i<10;i++){const a=i*Math.PI/5;c.beginPath();c.moveTo(m.x+Math.cos(a)*32,m.y+Math.sin(a)*32);c.lineTo(m.x+Math.cos(a)*48,m.y+Math.sin(a)*48);c.stroke();}}
  }
}
