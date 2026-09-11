import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import extension from "./index.ts";

test("role changes replace system prompt, disable selector tools and bound model turns", async () => {
    const dir=mkdtempSync(join(tmpdir(),"xgov-role-"));
    const config=join(dir,"role.json");
    const keys=["XGOVERNOR_BRIDGE_URL","XGOVERNOR_BRIDGE_TOKEN","XGOVERNOR_WORKSPACE_ROOT","XGOVERNOR_PI_ROLE_FILE"];
    const previous=Object.fromEntries(keys.map(key=>[key,process.env[key]]));
    Object.assign(process.env,{XGOVERNOR_BRIDGE_URL:"http://127.0.0.1:1",XGOVERNOR_BRIDGE_TOKEN:"test",XGOVERNOR_WORKSPACE_ROOT:dir,XGOVERNOR_PI_ROLE_FILE:config});
    try {
        const handlers=new Map();let active=[];let aborts=0;
        extension({on:(name,fn)=>handlers.set(name,fn),registerCommand:()=>{},registerTool:()=>{},setActiveTools:names=>{active=names;}});
        writeFileSync(config,JSON.stringify({system_prompt:"selector",tools_enabled:false,max_turns:2}));
        assert.deepEqual(await handlers.get("before_agent_start")(),{systemPrompt:"selector"});
        assert.deepEqual(active,[]);
        assert.equal((await handlers.get("tool_call")()).block,true);
        const ctx={abort:()=>{aborts++;}};
        await handlers.get("turn_start")({},ctx);await handlers.get("turn_start")({},ctx);assert.equal(aborts,0);
        await handlers.get("turn_start")({},ctx);assert.equal(aborts,1);
        writeFileSync(config,JSON.stringify({system_prompt:"solver-step",tools_enabled:true,max_turns:3}));
        assert.deepEqual(await handlers.get("before_agent_start")(),{systemPrompt:"solver-step"});
        assert.deepEqual(active,["read","write","edit","bash","find","grep"]);
        assert.equal(await handlers.get("tool_call")(),undefined);
        await handlers.get("turn_start")({},ctx);assert.equal(aborts,1,"counter resets at prompt boundary");
    } finally { for (const key of keys) {if(previous[key]===undefined)delete process.env[key];else process.env[key]=previous[key];}rmSync(dir,{recursive:true,force:true}); }
});
