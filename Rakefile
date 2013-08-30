require 'rspec/core/rake_task'
require 'rubygems/package_task'
require 'rake/rdoctask'
require 'rake/clean'

CLOBBER.include('coverage')

task :default => :spec

desc "Run all specs with rcov"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = ['--color', '--format documentation']
  t.pattern = 'spec/**/*_spec.rb'
  t.rcov = true
  t.rcov_opts = %q[--text-summary --exclude "__FORWARDABLE__,eval,jsignal_internal,spec/"]
end

if File.exist?('rta.gemspec')
  desc "Package into distributable tar, zip and gem files"
  load 'rta.gemspec'
  Gem::PackageTask.new(SPEC) do |pkg|
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
