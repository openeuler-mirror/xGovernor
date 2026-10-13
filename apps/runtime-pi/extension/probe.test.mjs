import assert from "node:assert/strict";
import {test} from "node:test";
import {createServer} from "node:http";
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {fileURLToPath} from "node:url";
import {spawn} from "node:child_process";
import {createInterface} from "node:readline";

export function respond(res,api){
    res.writeHead(200,{"content-type":"text/event-stream"});
    const send=(data,event)=>res.write(`${event?`event: ${event}\n`:""}data: ${JSON.stringify(data)}\n\n`);
    if(api==="openai-completions"){
        send({id:"chat-1",object:"chat.completion.chunk",created:1,model:"test-model",choices:[{index:0,delta:{role:"assistant",content:"OK"},finish_reason:null}]});
        send({id:"chat-1",object:"chat.completion.chunk",created:1,model:"test-model",choices:[{index:0,delta:{},finish_reason:"stop"}],usage:{prompt_tokens:1,completion_tokens:1,total_tokens:2}});
        res.write("data: [DONE]\n\n");
    }else if(api==="anthropic-messages"){
        for(const data of [
            {type:"message_start",message:{id:"msg-1",type:"message",role:"assistant",model:"test-model",content:[],stop_reason:null,usage:{input_tokens:1,output_tokens:0}}},
            {type:"content_block_start",index:0,content_block:{type:"text",text:""}},
            {type:"content_block_delta",index:0,delta:{type:"text_delta",text:"OK"}},
            {type:"content_block_stop",index:0},
            {type:"message_delta",delta:{stop_reason:"end_turn",stop_sequence:null},usage:{output_tokens:1}},
            {type:"message_stop"},
        ])send(data,data.type);
    }else{
        const item={id:"msg-1",type:"message",role:"assistant",status:"completed",content:[{type:"output_text",text:"OK",annotations:[]}]};
        const response={id:"resp-1",object:"response",created_at:1,model:"test-model",status:"completed",output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2,input_tokens_details:{cached_tokens:0},output_tokens_details:{reasoning_tokens:0}}};
        for(const data of [
            {type:"response.created",response:{...response,status:"in_progress",output:[]}},
            {type:"response.output_item.added",output_index:0,item:{...item,status:"in_progress",content:[]}},
            {type:"response.content_part.added",item_id:item.id,output_index:0,content_index:0,part:{type:"output_text",text:"",annotations:[]}},
            {type:"response.output_text.delta",item_id:item.id,output_index:0,content_index:0,delta:"OK"},
            {type:"response.output_item.done",output_index:0,item},
            {type:"response.completed",response},
        ])send(data,data.type);
    }
    res.end();
}

async function runProbe(config){
    const dir=mkdtempSync(join(tmpdir(),"xgov-probe-test-"));
    mkdirSync(join(dir,"agent"));
    writeFileSync(join(dir,"config.json"),JSON.stringify(config));
    // The startup catalogue needs to know custom model IDs before session_start.
    writeFileSync(join(dir,"agent","models.json"),JSON.stringify({providers:config.apiBase ? {[config.provider]:{
        baseUrl:config.apiBase,api:config.api??"openai-completions",apiKey:"${XGOVERNOR_PI_MODEL_API_KEY}",models:[{id:config.model}],
    }} : {}}));
    const child=spawn(process.env.PI_TEST_EXECUTABLE??"pi",["--mode","rpc","--no-session","--no-tools","--no-extensions","--no-skills","--no-context-files","--no-themes","--no-prompt-templates",
        "-e",fileURLToPath(new URL("probe.ts",import.meta.url))],
        {cwd:dir,env:{...process.env,PI_OFFLINE:"1",PI_CODING_AGENT_DIR:join(dir,"agent"),XGOVERNOR_PI_LLM_CONFIG:join(dir,"config.json"),XGOVERNOR_PI_MODEL_API_KEY:"test-secret"}});
    const lines=createInterface({input:child.stdout});let stderr="";child.stderr.on("data",data=>stderr+=data);
    let timer;
    try{
        return await new Promise((resolve,reject)=>{
            timer=setTimeout(()=>reject(new Error(`Probe timed out: ${stderr}`)),15000);
            child.on("error",reject);child.on("exit",()=>reject(new Error(`Pi exited: ${stderr}`)));
            lines.on("line",line=>{try{let event=JSON.parse(line);if(event.type==="extension_ui_request" && event.method==="notify")event=JSON.parse(event.message);if(event.type==="xgovernor_probe")resolve(event);}catch{}});
        });
    }finally{
        clearTimeout(timer);child.kill();lines.close();
        await new Promise(resolve=>child.exitCode!==null?resolve():child.once("exit",resolve));
        rmSync(dir,{recursive:true,force:true});
    }
}

for(const api of ["openai-completions","openai-responses","anthropic-messages"]){
    test(`real Pi probe uses ${api} wire format and authentication`,async()=>{
        let received;
        const server=createServer(async(req,res)=>{
            let data="";for await(const part of req)data+=part;
            received={url:req.url,headers:req.headers,body:JSON.parse(data)};
            respond(res,api);
        });
        await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
        try{
            const result=await runProbe({provider:"test-provider",model:"test-model",api,apiBase:`http://127.0.0.1:${server.address().port}${api==="anthropic-messages"?"":"/v1"}`});
            assert.equal(result.success,true,JSON.stringify(result));
            assert.equal(received.url,api==="openai-completions"?"/v1/chat/completions":api==="openai-responses"?"/v1/responses":"/v1/messages");
            assert.equal(received.body.model,"test-model");
            assert.equal(received.body.stream,true);
            if(api==="anthropic-messages")assert.equal(received.headers["x-api-key"],"test-secret");
            else assert.equal(received.headers.authorization,"Bearer test-secret");
        }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
    });
}

test("probe errors are classified and redacted",async()=>{
    let status=401;
    const server=createServer((_req,res)=>{res.writeHead(status,{"content-type":"application/json"});res.end(JSON.stringify({error:{message:"test-secret unavailable",type:"invalid_request_error"}}));});
    await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
    try{
        for(const [code,expected] of [[401,"llm_auth"],[404,"llm_model"],[500,"llm_provider"]]){
            status=code;
            const result=await runProbe({provider:"test-provider",model:"test-model",apiBase:`http://127.0.0.1:${server.address().port}/v1`});
            assert.equal(result.success,false);assert.equal(result.code,expected,JSON.stringify(result));
            assert.ok(!JSON.stringify(result).includes("test-secret"));
        }
    }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});

test("probe rejects unavailable catalogue models and conflicting protocols",async()=>{
    assert.equal((await runProbe({provider:"openai",model:"not-a-real-model"})).code,"llm_model");
    assert.equal((await runProbe({provider:"openai",model:"gpt-4o",api:"anthropic-messages"})).code,"llm_config");
});

test("stalled endpoint times out and connection failures are classified",{timeout:30000},async()=>{
    const server=createServer(()=>{});
    await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
    const apiBase=`http://127.0.0.1:${server.address().port}/v1`;
    try{
        const result=await runProbe({provider:"test-provider",model:"test-model",apiBase});
        assert.equal(result.code,"llm_timeout",JSON.stringify(result));
    }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
    const result=await runProbe({provider:"test-provider",model:"test-model",apiBase});
    assert.equal(result.code,"llm_connection",JSON.stringify(result));
});
