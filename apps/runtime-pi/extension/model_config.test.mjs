import assert from "node:assert/strict";
import {test} from "node:test";
import {resolveModelConfig, safeModelError} from "./model-config.ts";

test("custom protocols and legacy capabilities", async()=>{
    for(const api of [undefined,"openai-completions","openai-responses","anthropic-messages"]){
        const {model}=await resolveModelConfig({provider:"test",model:"test",apiBase:"http://localhost/v1",api});
        assert.equal(model.api,api??"openai-completions");
        assert.equal(model.contextWindow,128000);assert.equal(model.maxTokens,32000);
        assert.equal(model.reasoning,false);assert.deepEqual(model.input,["text"]);
    }
    const {model}=await resolveModelConfig({provider:"test",model:"test",apiBase:"http://localhost/v1",
        capabilities:{reasoning:true,input:["text","image"],context_window:8000,max_tokens:1000}});
    assert.equal(model.reasoning,true);assert.equal(model.contextWindow,8000);assert.equal(model.maxTokens,1000);
    assert.deepEqual(model.input,["text","image"]);
});
test("built-in capability inheritance, protocol conflict and merged validation",async()=>{
    const base={provider:"openai",model:"gpt-4o"};
    const original=(await resolveModelConfig(base)).model;
    const overridden=(await resolveModelConfig({...base,capabilities:{reasoning:true}})).model;
    assert.equal(overridden.api,original.api);assert.equal(overridden.contextWindow,original.contextWindow);
    assert.deepEqual(overridden.input,original.input);assert.equal(overridden.reasoning,true);
    const custom=(await resolveModelConfig({...base,apiBase:"http://localhost/v1",api:"anthropic-messages"})).model;
    assert.equal(custom.contextWindow,original.contextWindow);assert.equal(custom.api,"anthropic-messages");
    await assert.rejects(resolveModelConfig({...base,api:"anthropic-messages"}),/Protocol conflicts/);
    await assert.rejects(resolveModelConfig({...base,model:"not-a-model"}),/Model not found/);
    for(const capabilities of [{input:[]},{input:["audio"]},{context_window:1},{max_tokens:0},{reasoning:"yes"}]){
        await assert.rejects(resolveModelConfig({...base,capabilities}));
    }
    await assert.rejects(resolveModelConfig({...base,api:"unsupported"}),/Unsupported/);
});
test("model errors never expose upstream secrets or URLs",()=>{
    for(const detail of ["401 token-secret https://private", "404 token-secret", "connection token-secret", "timeout token-secret", "500 token-secret"]){
        assert.ok(!JSON.stringify(safeModelError(new Error(detail))).includes("token-secret"));
    }
});
