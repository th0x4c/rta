SPEC = Gem::Specification.new do |s|
  s.name          = "rta"
  s.summary       = "Database transaction application generation tool in JRuby."
  s.description   = <<-EOF
    RTA is a database transaction application generation tool in JRuby.
    It enables the user to generate transaction processing application that is
    written in JRuby and JDBC.
  EOF
  s.version       = "0.3.8"
  s.author        = "Takashi Hashizume"
  s.email         = "th0x4c@gmail.com"
  s.files         = Dir.glob("{bin,lib,spec,examples}/**/*") + ['README', 'Rakefile']
  s.require_paths = ['lib']
  s.executables   = ['rtactl', 'rtactl.rb']
  s.has_rdoc      = true
  s.extra_rdoc_files = ["README"]
  s.homepage      = "https://github.com/th0x4c/rta"
end
