require 'rake/testtask'

Rake::TestTask.new do |task|
  task.libs << "test"
  task.libs << "test/conf-test"
  task.test_files = FileList['test/test*.rb']
  task.verbose = false
end

task :build do
  system "gem build corm.gemspec"
end

task :default => :test
