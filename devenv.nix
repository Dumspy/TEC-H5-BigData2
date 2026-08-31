{ pkgs, lib, ... }:
{
  env.UV_PROJECT_ENVIRONMENT = lib.mkForce ".venv";

  languages.python = {
    enable = true;
    uv.enable = true;
  };
}
