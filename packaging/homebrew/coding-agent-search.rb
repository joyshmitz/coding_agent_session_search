class CodingAgentSearch < Formula
  desc "Unified TUI search over local coding agent histories"
  homepage "https://github.com/Dicklesworthstone/coding_agent_session_search"
  version "0.1.64"
  license :cannot_represent

  on_macos do
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v0.1.64/cass-darwin-arm64.tar.gz"
      sha256 "797cd64b7e88171985480963fbcc07045b678bffc9a069904fd34c0ac938bfd7"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v0.1.64/cass-linux-amd64.tar.gz"
      sha256 "6ea31940ef70286b598ed35e665ab20d3b7424a3ae36fa92b3ea010bca509165"
    end
    on_arm do
      url "https://github.com/Dicklesworthstone/coding_agent_session_search/releases/download/v0.1.64/cass-linux-arm64.tar.gz"
      sha256 "9d41d63bbfdaa2506284830f73e1723dcdceacc337b03e49cabfd430c74f25ee"
    end
  end

  def install
    bin.install "cass"
  end

  test do
    system "#{bin}/cass", "--help"
  end
end
