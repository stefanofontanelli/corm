Gem::Specification.new do |s|
  s.name = 'corm'
  s.version = `cat #{File.dirname(__FILE__)}/VERSION`
  s.authors = ['Stefano Fontanelli']
  s.email = ['stefano@gild.com']
  s.homepage = 'https://github.com/stefanofontanelli/corm'
  s.summary = 'Very basic Cassandrea ORM which is rails-deps-free'
  s.description = ''
  s.files = `git ls-files | grep lib`.split("\n")
  s.executables = `git ls-files -- bin/*`.split("\n").map{|i| i.gsub(/^bin\//,'')}
  s.add_dependency 'cassandra-driver', '~> 2.0.1', '>= 2.0.1'
  s.add_dependency 'multi_json', '~> 1.10.1', '>= 1.10.1'
  s.add_development_dependency 'rake', '~> 10.0.0', '>= 10.0.0'
end
