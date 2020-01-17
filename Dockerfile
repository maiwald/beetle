FROM ruby:2.6.5-buster

# Install ruby gems
ADD Gemfile /home/mercury/app/Gemfile
ADD Gemfile.lock /home/mercury/app/Gemfile.lock
ADD beetle_etl.gemspec /home/mercury/app/beetle_etl.gemspec
WORKDIR /home/mercury/app
RUN gem install bundler && bundle