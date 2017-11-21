require 'mondrian-olap'
require "vertica-jdbc-8.0.1-6.jar"

class Article
  def self.schema
    @schema ||= Mondrian::OLAP::Schema.define do
      cube 'Articles' do
        table 'vt_fact_article_events'
        cache false

        dimension 'Device', foreign_key: 'device_id' do
          hierarchy has_all: true, all_member_name: 'All Devices', primary_key: 'id' do
            table 'vt_dim_devices'
            level 'Device Type', column: 'device_type', unique_members: false
          end
        end

        dimension 'Location', foreign_key: 'location_id' do
          hierarchy has_all: true, all_member_name: 'All Locations', primary_key: 'id' do
            table 'vt_dim_locations'
            level 'Continent', column: 'continent', unique_members: false
            level 'Country', column: 'country', unique_members: false
            level 'Subdivision', column: 'subdivision', unique_members: false
            level 'City', column: 'city', unique_members: false
          end
        end

        measure 'Views', column: 'views', aggregator: 'sum'
      end
    end
  end

  def self.olap
    Mondrian::OLAP::Connection.create(
      driver: 'jdbc',
      #jdbc_url: 'jdbc:vertica://52.44.235.220:5433/?user=ds_readonly&password=ds123_analytics',
      jdbc_url: 'jdbc:vertica://vertica-stagingvpc-017.aws.simplereach.com/?user=dbadmin&password=simplereach_analytics',
      jdbc_driver: 'com.vertica.jdbc.Driver',
      schema: schema
    )
  end
end
