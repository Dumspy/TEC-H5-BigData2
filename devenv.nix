{ pkgs, lib, ... }:
{
  env.UV_PROJECT_ENVIRONMENT = lib.mkForce ".venv";

  # PySpark needs a JVM for its driver even when the Spark engine runs on the
  # cluster.
  packages = [ pkgs.jdk17_headless ];
  env.JAVA_HOME = "${pkgs.jdk17_headless}";

  languages.python = {
    enable = true;
    uv.enable = true;
  };
}
