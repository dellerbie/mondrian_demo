require "vertica-jdbc-7.2.3-0.jar"

# # will have to play around with defining the schema
schema = Mondrian::OLAP::Schema.define do
  cube 'Articles' do
    table 'vt_fact_article_events'

    dimension 'Location', foreign_key: 'location_id' do
      hierarchy has_all: true, all_member_name: 'All Locations', primary_id: 'id' do
        table 'vt_dim_locations'
        level 'Continent', column: 'continent', unique_members: false
      end
    end

    dimension 'Device', foreign_key: 'device_id' do
      hierarchy has_all: true, all_member_name: 'All Devices', primary_id: 'id' do
        table 'vt_dim_devices'
        level 'Device Type', column: 'device_type', unique_members: false
      end
    end

    measure 'Views', column: 'views', aggregator: 'sum'
  end
end

olap = Mondrian::OLAP::Connection.create(
  driver: 'jdbc',
  jdbc_url: 'jdbc:vertica://52.44.235.220:5433/?user=ds_readonly&password=ds123_analytics',
  jdbc_driver: 'com.vertica.jdbc.Driver',
  schema: schema
)

# begin
#   olap.execute <<-MDX
#     SELECT 
#       [Device].[Device Type].Members ON 0
#     FROM 
#       [Articles]
#   MDX
# rescue Mondrian::OLAP::Error => e
#   puts e.root_cause
#   puts e.backtrace
# end

begin
  # cube = olap.cube('Articles')
  # puts cube.dimension_names
  # puts cube.dimension('Location').hierarchy.child_names
  result = olap.execute <<-MDX
    SELECT   
        {[Measures].[Views]} ON COLUMNS,
        {[Device].children} ON ROWS
    FROM 
        [Articles] 
  MDX


  # result = olap.from('Articles').columns('[Location].children').execute
  puts result.row_names
  puts result.values
rescue Mondrian::OLAP::Error => e
  puts e.root_cause
  # puts e.backtrace
end

# # begin
# #   olap.from('Articles').columns('[Measures].[Views]').rows('[Devices].members').execute
# # rescue Mondrian::OLAP::Error => e
# #   puts e.root_cause
# # end

# "SELECT SUM(views) views_sum FROM vt_fact_article_events LEFT JOIN vt_dim_locations ON vt_fact_article_events.location_id = vt_dim_locations.id LEFT JOIN vt_dim_devices ON vt_fact_article_events.device_id = vt_dim_devices.id WHERE vt_dim_devices.device_type = 'SmartPhone' GROUP BY vt_dim_locations.country ORDER BY vt_dim_locations.country"

# SELECT
#   d.device_type,
#   sum(views) as page_views
# FROM
#   vt_fact_article_events
# JOIN
#   vt_dim_devices as d ON d.id = device_id
# WHERE
#   time_stamp >= '2017-11-06 12:00' and time_stamp < '2017-11-06 13:00:00'
# GROUP BY
#   d.device_type;


# board_id = 999351628602949634


