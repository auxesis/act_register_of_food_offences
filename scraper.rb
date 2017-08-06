require 'scraperwiki'
require 'geokit'
require 'pdf-reader'
require 'pry'
require 'nokogiri'
require 'active_support/core_ext/string'

# Set an API key if provided
Geokit::Geocoders::GoogleGeocoder.api_key = ENV['MORPH_GOOGLE_API_KEY'] if ENV['MORPH_GOOGLE_API_KEY']

class String
  def to_md5
    Digest::MD5.new.hexdigest(self)
  end

  def scrub!
    self.gsub!(/[[:space:]]/, ' ') # convert all utf whitespace to simple space
    self.strip!
  end
end

def logging_method_name
  caller[1][/`(block in )*(.+)'/,2] + ': '
end

def info(message)
  puts '[info] ' + logging_method_name + message
end

def debug(message)
  puts '[debug] ' + logging_method_name + message
end

def generate_id(record)
  blacklist_keys = %w(id link lat lng)
  return Hash[record.reject {|k,v| blacklist_keys.include?(k)}].map(&:to_s).join(' ').to_md5
end

def geocode(prosecution)
  @addresses ||= {}

  address = [
    prosecution['business_address'],
    'Canberra',
    'ACT'
  ].join(', ')

  if @addresses[address]
    info('Geocoding [cache hit] ' + address)
    location = @addresses[address]
  else
    info('Geocoding ' + address)
    a = Geokit::Geocoders::GoogleGeocoder.geocode(address)

    debug('Could not geocode ' + address) if !a.lat && !a.lng

    location = {
      'lat' => a.lat,
      'lng' => a.lng,
    }

    @addresses[address] = location
  end

  prosecution.merge!(location)
end

def existing_record_ids
  return @cached if @cached
  @cached = ScraperWiki.select('id from data').map {|r| r['id']}
rescue SqliteMagic::NoSuchTable
  []
end

def base
  'http://www.health.act.gov.au'
end

def path
  '/sites/default/files//Register_of_Food_Offences_20June_2017ppm.pdf'
end

def url
  base + path
end

def last_known_link_text
  'Register of Food Offences (updated 20 June 2017)'
end

# ACT health don't publish the PDF of the register of offences at a consistent
# URL – it changes every time they publish a new one one. This method checks if
# the PDF being linked to is different to the last known PDF.
#
# Looking at past copies of this page on the Wayback Machine, we know that:
#
# - The link text changes
# - The link URL changes
#
# These are the URLs that have been used previously:
#
# - http://www.health.act.gov.au/sites/default/files/Register%20of%20Food%20Offences%20%28Last%20updated%2016%20March%202015%29.pdf
# - http://health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences%20%28updated%2017%20Sep%202015%29.pdf
# - http://health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences.pdf
# - http://www.health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences.pdf
# - http://www.health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences_0.pdf
# - http://www.health.act.gov.au/sites/default/files//Register_of_Food_Offences_20June_2017ppm.pdf
#
# Note that some of them are duplicate, so there's no guarantee that if the
# resource URL has not changed, the resource is not the same.
#
# It will exit if:
#
#  - The register link is not on the page. This could happen because the website
#    is broken, the URL has changed, or the text they use in the link to the PDF
#    has changed.
#  - The URL to the PDF doesn't match the last known URL.
#  - The link text doesn't match the last known link text.
#
# This is all a bit shit, because I don't want to have to update the scraper
# every time the URL changes. ¯\_(ツ)_/¯
#
def abort_if_updated?
  index = base + '/public-information/businesses/food-safety-regulation/register-food-offences'
  page = open(index)
  doc = Nokogiri::HTML(page)
  register_link = doc.search('a').find {|a| a.text =~ /register.*updated/i }
  register_link_text = register_link.text.strip

  case
  when !register_link
    info('Could not find register link. Page source:')
    info("\n" + doc + "\n")
    info('Exiting!')
    exit(1)
  when register_link_text != last_known_link_text
    info('Link text has changed!')
    info('Expected: ' + last_known_link_text)
    info('Actual:   ' + register_link_text)
    info('Exiting!')
    exit(2)
  when register_link['href'] != path
    info('New register published at ' + register_link['href'])
    info('Exiting!')
    exit(3)
  end
end

def extract_header(page)
  page.find {|l| l =~ /^Prosecution Details/}
end

# Work out column names, and where they start and stop on each page
def determine_columns(page)
  columns = {}

  header = extract_header(page)
  names = header.split(/\s{2,}/)

  names.each_cons(2) do |(first,last)|
    start = header.index(first)
    stop  = header.index(last) - 1
    columns[first] = (start..stop)
  end

  start = header.index(names.last)
  stop  = -1
  columns[names.last] = (start..stop)

  columns
end

# Strip header and footer from page, to return just the prosecutions
def get_raw_lines(page)
  header = extract_header(page)
  start = page.index(header) + 1
  stop = -2 # last line is always a footer

  page[start..stop]
end

def end_of_record?(line)
  line =~ /Total \(\d+\) Charge/i
end

def finalise_record!
  # Prosecution Details
  values = @record.delete('Prosecution Details') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  @record['prosecution_details'] = values.join(' ')

  # Business Address
  values = @record.delete('Business Address') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  @record['business_address'] = values.join(' ')

  # Date of Offence
  values = @record.delete('Date of Offence') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  values.map! {|v| Date.parse(v).to_s}
  @record['offence_dates'] = values

  # Offence Proven
  values = @record.delete('Offence Proven') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  values.reject! {|v| v =~ /Total \(\d+\) Charge/i}
  @record['offence_proven'] = values

  # Imposed Penalty
  values = @record.delete('Imposed Penalty') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  @record['imposed_penalties'] = values

  # Removal Date
  values = @record.delete('Removal date') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  @record['removal_date'] = Date.parse(values.join(' ')).to_s

  # Notes
  values = @record.delete('Notes') || []
  values.compact!
  values.map!(&:strip).reject! {|v| v.blank?}
  @record['notes'] = values.join(' ')

  @records << @record
  @record = nil
end

# Normalise the "Imposed Penalties" field across all records.
#
# There is an edge case where the dollar value normally expected on a
# "Total (x) Charges" termination line is on the next line. Because we
# use that line as the record terminator, the dollar value is pushed into the
# next record.
#
# This method:
#
#  - Removes the dollar value from the "Total (x) Charges" line, because it's
#    just a total that the user can automatically compute.
#  - Deletes the "Total (x) Charges" dollar value that sometimes gets put into
#    the next record.
#
def clean_imposed_penalties!
  @records.each do |record|
    record['imposed_penalties'].select! {|value| value =~ /\$\d*,*\d+$/}
  end

  @records.each_with_index do |record, index|
    offset = record['imposed_penalties'].size - record['offence_proven'].size
    case offset
    # Default case, where there's a total at the end that must be deleted
    when 1
      record['imposed_penalties'].delete_at(-1)
    # Edge case, where total has been spread across multiple lines
    when 0
      next_record = @records[index+1]
      next_record['imposed_penalties'].delete_at(0) if next_record
    # Unhandled case, where the data has changed significantly
    else
      raise "Unhandled offset: #{offset}: #{record.inspect}"
    end
  end
end

# TODO(auxesis): split offences into individual records
def split_records_into_multiple_prosecutions(records)
  prosecutions = []

  records.each do |record|
    offences = record['offence_proven']
    penalties = record['imposed_penalties']
    offences.zip(penalties).each do |offence,penalty|
      prosecution = {
        'business_name'    => record['prosecution_details'],
        'business_address' => record['business_address'],
        'offence_date'     => record['offence_dates'].first, # because it's nearly impossible to match offence dates
        'offence'          => offence,
        'imposed_penalty'  => penalty,
        'removal_date'     => record['removal_date'],
        'notes'            => record['notes'],
        'link'             => url,
      }
      prosecution['id'] = generate_id(prosecution)

      prosecutions << prosecution
    end
  end

  prosecutions
end

def add_to_record(column, value)
  # Collection of all records
  @records ||= []

  # The record currently being processed
  @record ||= {}

  @record[column] ||= []
  @record[column] << value
end

def build_records(raw_lines, columns)
  raw_lines.each do |line|
    columns.each do |column, range|
      string = line[range]
      add_to_record(column, string)
    end
    finalise_record! if end_of_record?(line)
  end
end

# Iterate through all pages to build up records
def extract_records_from_pages(pages)
  pages.each do |page|
    columns   = determine_columns(page)
    raw_lines = get_raw_lines(page)
    build_records(raw_lines, columns)
  end

  clean_imposed_penalties!

  @records
end

# Strips out empty lines
def clean_pages(pages)
  pages.map { |page| page.text.split("\n").reject {|l| l=~ /^\s*$/} }
end

def fetch_and_build_prosecutions
  io = open(url)
  reader = PDF::Reader.new(io)

  pages = clean_pages(reader.pages)
  records = extract_records_from_pages(pages)
  split_records_into_multiple_prosecutions(records)
end

def fix_ids
  records = ScraperWiki.select('* from data')
  original_record_ids = records.map {|r| r['id']}

  records.each { |record| record['id'] = generate_id(record) }
  records.uniq! { |record| record['id'] }
  fixed_record_ids = records.map {|r| r['id']}

  if (original_record_ids - fixed_record_ids).size > 0
    info('Number of records to fix: ' + original_record_ids.size)
    info('Number of records after fix: ' + fixed_record_ids.size)

    info('Deleting old records!')
    ScraperWiki.sqliteexecute('DELETE FROM data')

    info('Saving new records!')
    ScraperWiki.save_sqlite(['id'], records)
    saved_record_ids = ScraperWiki.select('id from data').map {|r| r['id']}

    info('Number of records after save: ' + saved_record_ids.size)

    if fixed_record_ids.size != saved_record_ids.size
      info("Error: Fixed #{fixed_record_ids.size} and saved #{saved_record_ids.size} do not match!")
      exit(2)
    end
  else
    info('There are no records to fix.')
  end
end

# This is useful because ACT health removes the all-but-current PDF.
def save_to_wayback_machine
  info('Saving target PDF to the Wayback Machine.')
  require 'net/http'

  save_url = 'http://web.archive.org/save/' + url
  uri = URI(save_url)

  Net::HTTP.start(uri.host, uri.port) do |http|
    request = Net::HTTP::Get.new(uri)
    response = http.request(request)
    if response.class != Net::HTTPFound
      info("Attempt to save #{url} to Wayback Machine failed.")
      info('Checking if the linked PDF is different to the last known PDF.')
      abort_if_updated?
      info("The PDF hasn't changed, but the PDF is no longer there.")
      info("Exiting!")
      exit(2)
    end
  end
end

def main
  # Fix up ids after design flaw in algorithm to generate id
  fix_ids

  # Ping the wayback machine to save a copy of the PDF.
  save_to_wayback_machine

  # The normal scraper run
  prosecutions = fetch_and_build_prosecutions

  info("### Found #{prosecutions.size} notices")
  new_prosecutions = prosecutions.select {|r| !existing_record_ids.include?(r['id'])}
  info("### There are #{new_prosecutions.size} new prosecutions")
  new_prosecutions.map! {|p| geocode(p) }

  # Serialise
  ScraperWiki.save_sqlite(['id'], new_prosecutions)

  info('Done')
end

main()
