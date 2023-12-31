from conans import ConanFile

class ConanPackage(ConanFile):
  name = 'network-monitor'
  version = '0.1.0'
  generators = 'cmake_find_package'
  
  requires = [
    ('boost/1.74.0'),
    ('openssl/3.1.1'),
    ('libcurl/8.1.2')
  ]

  default_options = (
    'boost:shared=False'
  )