#!/usr/bin/ruby1.9.1

require 'rubygems'
require 'net/github-upload'

login = `git config github.user`.chomp
token = `git config github.token`.chomp

gh = Net::GitHub::Upload.new(
    :login => login,
    :token => token
)

direct_link = gh.replace(
    :repos => "Incubaid/arakoon",
    :file => `pwd`.chomp + "/doc/_build/latex/Arakoon.pdf",
    :name => "arakoon-default.pdf",
    :content_type => "application/pdf",
    :description => "Arakoon documentation from 'default' branch"
)
