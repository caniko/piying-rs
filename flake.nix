{
  description = "Rust project";

  inputs = {
    harbor-rs.url = "git+https://github.com/caniko/harbor-rs.git?ref=trunk&rev=05cc4f162b55fa904b687db1821e2463fa813e50";
    rs-harbor.follows = "harbor-rs";
    nixpkgs.follows = "harbor-rs/nixpkgs";
    rust-overlay.follows = "harbor-rs/rust-overlay";
    crane.follows = "harbor-rs/crane";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    git-hooks.url = "github:cachix/git-hooks.nix";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    crane,
    flake-utils,
    treefmt-nix,
    git-hooks,
    harbor-rs,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [(import rust-overlay)];
      };

      toolchain = harbor-rs.lib.mkToolchain {
        inherit pkgs;
        channel = "stable";
        extensions = ["rustfmt" "clippy" "rust-src"];
      };
      inherit (toolchain) rustToolchain craneLib;
      buildCache = harbor-rs.lib.mkBuildCachePolicy {
        inherit pkgs;
        sccachePackage = harbor-rs.packages.${system}.sccache;
        cacheRoot = null;
        namespaceScope = "canix-rust";
        namespaceGeneration = 5;
      };
      src = pkgs.lib.cleanSourceWith {
        src = ./.;
        filter = path: type:
          (craneLib.filterCargoSources path type)
          || builtins.baseNameOf path == "README.md";
      };
      commonArgs = {
        inherit src;
        strictDeps = true;
      };
      cargoArtifacts = buildCache.withRustCache {package = craneLib.buildDepsOnly commonArgs;};
      package = buildCache.withRustCache {package = craneLib.buildPackage (commonArgs // {inherit cargoArtifacts;});};
      treefmtEval = treefmt-nix.lib.evalModule pkgs (import ./nix/treefmt.nix);
      pre-commit-check = git-hooks.lib.${system}.run {
        src = ./.;
        hooks = import ./nix/pre-commit.nix {
          inherit pkgs;
          treefmtWrapper = treefmtEval.config.build.wrapper;
          inherit rustToolchain;
        };
      };
    in {
      packages.default = package;
      formatter = treefmtEval.config.build.wrapper;
      checks = {
        default = package;
        formatting = treefmtEval.config.build.check self;
        clippy = craneLib.cargoClippy (commonArgs
          // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets --all-features -- --deny warnings";
          });
        fmt = craneLib.cargoFmt {inherit src;};
      };
      devShells.default = craneLib.devShell {
        checks = self.checks.${system};
        packages = with pkgs;
          [
            cargo-audit
            cargo-nextest
            pre-commit
            rust-analyzer
          ]
          ++ pre-commit-check.enabledPackages;
        shellHook = pre-commit-check.shellHook;
      };
    });
}
