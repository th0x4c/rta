require 'spec/rake/spectask'

task :default => :spec

desc "Run all specs with rcov"
Spec::Rake::SpecTask.new(:spec) do |t|
  t.spec_opts = ['--color', '--format specdoc']
  t.spec_files = FileList['spec/**/*_spec.rb']
  t.rcov = true
  t.rcov_opts = ['--text-summary', '--exclude "__FORWARDABLE__,eval"']
end
