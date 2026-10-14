use super::*;

pub(super) const BOOTSTRAP: &str = "io.xgovernor.bootstrap";

pub(super) fn bootstrapped(value: &Value) -> bool {
    value["Config"]["Labels"][BOOTSTRAP] == "v1"
}

impl DockerProvider {
    pub(super) async fn bootstrap_container(
        &self,
        name: &str,
        workspace: &str,
    ) -> Result<(), ProviderControlError> {
        // Only a trusted provider initialization can exec as root. The container's
        // default user remains 10001, with no effective/permitted capabilities.
        let probe = self.config.checked(args(&[
            "exec", "--user", "0:0", "--workdir", "/", name, "/bin/sh", "-c",
            "for p in /usr/local/bin/python3 /usr/bin/python3 /opt/miniconda3/bin/python; do if test -x \"$p\" && \"$p\" -I -c 'import sys; assert sys.version_info >= (3, 11)' 2>/dev/null; then printf '%s' \"$p\"; exit 0; fi; done; echo 'Docker bootstrap requires Python 3.11+ in the image' >&2; exit 1",
        ])).await?;
        let python = String::from_utf8(probe.stdout).map_err(transport)?;
        if !matches!(
            python.as_str(),
            "/usr/local/bin/python3" | "/usr/bin/python3" | "/opt/miniconda3/bin/python"
        ) {
            return Err(invalid("invalid bootstrap Python interpreter"));
        }
        let output = self
            .config
            .run(
                &args(&[
                    "exec",
                    "-i",
                    "--user",
                    "0:0",
                    "--workdir",
                    "/",
                    name,
                    &python,
                    "-I",
                    "-c",
                    include_str!("bootstrap.py"),
                    workspace,
                ]),
                Some(include_bytes!("../../../../helper.py").to_vec()),
                300_000,
            )
            .await?;
        if !output.status.success() {
            return Err(transport(format!(
                "Docker bootstrap failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }
}
