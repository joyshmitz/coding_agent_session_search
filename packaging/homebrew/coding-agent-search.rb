class Cass < Formula
  desc "Cross-agent session search for AI coding conversations"
  homepage "https://github.com/Dicklesworthstone/coding_agent_session_search"
  version "0.2.2"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-darwin-arm64.tar.gz"
      sha256 "7fa79cb08052b54143bd50efd803f3045f13b71fe6d34a64525856dd2c21a832"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-linux-amd64.tar.gz"
      sha256 "71e489eaa4d21a78649a0ccfc3bf892caa1492756a9e96c6ce31a47d08794033"
    end
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v#{version}/cass-linux-arm64.tar.gz"
      sha256 "a29f12eb6170fd303ffc8f08093e947297f937a9bfed9a8dd63448a9825d46ad"
    end
  end

  def install
    bin.install "cass"
    generate_completions_from_executable(bin/"cass", "completions")
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/cass --version")
    assert_match "health", shell_output("#{bin}/cass --help")
  end
end
