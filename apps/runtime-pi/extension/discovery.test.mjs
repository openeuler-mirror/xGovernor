import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

test("real Pi disables discovery but loads the explicit bridge extension", {timeout:20000}, async () => {
    const dir=mkdtempSync(join(tmpdir(),"xgov-discovery-"));
    mkdirSync(join(dir,".pi","extensions"),{recursive:true});
    const marker=join(dir,"unexpected-extension");
    writeFileSync(join(dir,".pi","extensions","unwanted.ts"),`import {writeFileSync} from 'node:fs'; export default function(){writeFileSync(${JSON.stringify(marker)},'loaded');}`);
    const child=spawn(process.env.PI_TEST_EXECUTABLE ?? "pi",[
        "--mode","rpc","--no-extensions","--no-skills","--no-context-files",
        "--no-prompt-templates","--no-themes","--no-session","-e",fileURLToPath(new URL("index.ts",import.meta.url)),
    ],{cwd:dir,env:{...process.env,PI_OFFLINE:"1",PI_CODING_AGENT_DIR:join(dir,"agent"),
        XGOVERNOR_BRIDGE_URL:"http://127.0.0.1:1",XGOVERNOR_BRIDGE_TOKEN:"test",XGOVERNOR_WORKSPACE_ROOT:dir}});
    let stderr="";child.stderr.on("data",data=>stderr+=data);
    const lines=createInterface({input:child.stdout});
    try {
        const reply=new Promise((resolve,reject)=>{
            child.on("error",reject);
            child.on("exit",()=>reject(new Error(`Pi exited: ${stderr}`)));
            lines.on("line",line=>{try{const item=JSON.parse(line);if(item.id==="commands")resolve(item);}catch{}});
        });
        child.stdin.write(JSON.stringify({id:"commands",type:"get_commands"})+"\n");
        const response=await reply;
        assert.equal(response.success,true,stderr);
        assert.ok(response.data.commands.some(command=>command.name==="xgovernor-model"),stderr);
        assert.equal(existsSync(marker),false);
    } finally {
        child.kill(); lines.close();
        await new Promise(resolve=>child.exitCode!==null ? resolve() : child.once("exit",resolve));
        rmSync(dir,{recursive:true,force:true});
    }
});
