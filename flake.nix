{
  description = "Rust project";

  inputs = {
    harbor-rs.url = "git+https://github.com/caniko/harbor-rs.git?ref=trunk&rev=05cc4f162b55fa904b687db1821e2463fa813e50";
    rs-harbor.follows = "harbor-rs";
    nixpkgs.follows = "harbor-rs/nixpkgs";
    rust-overlay.follows = "harbor-rs/rust-overlay";
    crane.follows = "harbor-rs/crane";
    # No nixpkgs/rust-overlay/crane/harbor follows: the dev shell must reuse
    # Simit's own derivation so it substitutes from the public canix Attic cache.
    simit = {
      url = "git+https://github.com/caniko/simit.git?ref=trunk&rev=9ffacf22c9b69385e370846ea55ce6226a26b8ad";
      inputs.flake-utils.follows = "flake-utils";
    };
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    git-hooks.url = "github:cachix/git-hooks.nix";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
    treefmt-nix,
    git-hooks,
    harbor-rs,
    simit,
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
      simitPackage = simit.packages.${system}.default;
      treefmtEval = treefmt-nix.lib.evalModule pkgs (import ./nix/treefmt.nix {
        rustfmtPackage = pkgs.rust-bin.nightly.latest.default.override {
          extensions = ["rustfmt"];
        };
      });
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
            just
            pre-commit
            rust-analyzer
          ]
          ++ [simitPackage]
          ++ pre-commit-check.enabledPackages;
        inherit (pre-commit-check) shellHook;
      };
    });
}
