import {readFile,writeFile,mkdir} from 'node:fs/promises';
const base=new URL('./',import.meta.url),root=new URL('../',base);
const games=JSON.parse(await readFile(new URL('games.json',base),'utf8'));
const ids=new Set(['helicopter']);
for(const g of games){if(!/^[a-z0-9-]+$/.test(g.id)||ids.has(g.id)||typeof g.title!=='string'||!g.title.trim()||g.title.length>80)throw new Error('Invalid or duplicate game entry');const u=new URL(g.url);if(u.protocol!=='https:')throw new Error('Game URLs must use HTTPS');ids.add(g.id);}
const source=(await readFile(new URL('game.mjs',base),'utf8')).replaceAll('export ','');
const config=`const API_ROOT='https://api.discordchats.com/api';const ARCADE_GAMES=${JSON.stringify(games).replaceAll('<','\\u003c')};`;
const hub=(await readFile(new URL('index.template.html',base),'utf8')).replace('// __GAME_SOURCE__',()=>source).replace('// __CONFIG__',()=>config);
const game=(await readFile(new URL('helicopter.template.html',base),'utf8')).replace('// __GAME_SOURCE__',()=>source);
await mkdir(new URL('games/',root),{recursive:true});
await writeFile(new URL('arcade.html',root),hub);
await writeFile(new URL('games/helicopter.html',root),game);
console.log('Built arcade.html and games/helicopter.html.');
