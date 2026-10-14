#!/bin/bash
# `DeploymentProfile.resource_class` 属于请求 schema，会经 serde 传进来，但目前没有任何
# 代码按它选资源。客户端填了它就应当得到反馈，而不是以为这个选择生效了。
#
#     bash scripts/check-deployment-resource-class-is-reported.sh
set -u
F=crates/core/src/application.rs
if ! grep -q 'request\.deployment\.resource_class\.is_some()' "$F"; then
    echo "FAIL  application.rs 没有检查 deployment.resource_class —— 客户端传了它不会有任何反馈" >&2
    exit 1
fi
if ! grep -A 8 'request\.deployment\.resource_class\.is_some()' "$F" | grep -q 'tracing::warn!'; then
    echo "FAIL  resource_class 的检查没有给出提示 —— 静默忽略仍然存在" >&2
    exit 1
fi
echo "全部通过（resource_class 被设置时会给出提示）"
