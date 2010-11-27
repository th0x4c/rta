require 'spec/rake/spectask'
require 'rake/gempackagetask'
require 'rake/rdoctask'

task :default => :spec

desc "Run all specs with rcov"
Spec::Rake::SpecTask.new(:spec) do |t|
  t.spec_opts = ['--color', '--format specdoc']
  t.spec_files = FileList['spec/**/*_spec.rb']
  t.rcov = true
  t.rcov_opts = ['--text-summary', '--exclude "__FORWARDABLE__,eval"']
end

if File.exists?('rta.gemspec')
  desc "Package into distributable tar, zip and gem files"
  load 'rta.gemspec'
  Rake::GemPackageTask.new(SPEC) do |pkg|
    pkg.need_zip = true
    pkg.need_tar = true
  end
end

desc "Generate documentation"
Rake::RDocTask.new do |t|
  t.rdoc_dir = 'doc'
  t.rdoc_files.include('README', 'lib/**/*.rb')
  t.options = ['--inline-source', '--all', '--line-numbers', '--title', "RTA Documentation"]
end

desc "Generate documentation by yard"
require 'yard'
YARD::Rake::YardocTask.new do |t|
  t.options += ['--title', "RTA Documentation"]
  t.files = FileList['lib/**/*.rb']
end
