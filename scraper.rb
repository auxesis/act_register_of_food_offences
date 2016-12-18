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

def generate_id(record)
  return record.map(&:to_s).join(' ').to_md5
end

def geocode(prosecution)
  @addresses ||= {}

  address = [
    prosecution['business_address'],
    'Canberra',
    'ACT'
  ].join(', ')

  if @addresses[address]
    puts "Geocoding [cache hit] #{address}"
    location = @addresses[address]
  else
    puts "Geocoding #{address}"
    a = Geokit::Geocoders::GoogleGeocoder.geocode(address)

    if !a.lat && !a.lng
      puts "[debug] Couldn't geocode #{address}"
    end

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
  '/sites/default/files//Register%20of%20Food%20Offences_5.pdf'
end

def url
  base + path
end

def last_known_link_text
  'Register of Food Offences (updated 18 November 2016)'
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
# - http://www.health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences.pdf
# - http://www.health.act.gov.au/sites/default/files//Register%20of%20Food%20Offences_0.pdf
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
    puts '[fatal] Could not find register link. Page source:'
    puts "\n" + doc + "\n"
    puts '[fatal] Exiting!'
    exit 1
  when register_link_text != last_known_link_text
    puts '[fatal] Link text has changed!'
    puts '[fatal] Expected: ' + last_known_link_text
    puts '[fatal] Actual:   ' + register_link_text
    puts '[fatal] Exiting!'
    exit 2
  when register_link['href'] != path
    puts '[fatal] New register published at ' + register_link['href']
    puts '[fatal] Exiting!'
    exit 3
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
  values.map! {|v| Date.parse(v)}
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
  @record['removal_date'] = Date.parse(values.join(' '))

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
  @records.each_with_index do |record, index|
    offset = record['imposed_penalties'].size - record['offence_proven'].size
    case offset
    # Default case, where there's a total at the end that must be deleted
    when 1
      record['imposed_penalties'].delete_at(-1)
    # Edge case, where total has been spread across multiple lines
    when 0
      next_record = @records[index+1]
      next_record['imposed_penalties'].delete_at(0)
    # Unhandled case, where the data has changed significantly
    else
      raise "Unhandled offset: #{offset} #{record.inspect}"
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
        'offence_date'     => record['offence_dates'].first,
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
  abort_if_updated?

  io = open(url)
  reader = PDF::Reader.new(io)

  pages = clean_pages(reader.pages)
  records = extract_records_from_pages(pages)
  prosecutions = split_records_into_multiple_prosecutions(records)
end

def main
  prosecutions = fetch_and_build_prosecutions

  puts "### Found #{prosecutions.size} notices"
  new_prosecutions = prosecutions.select {|r| !existing_record_ids.include?(r['id'])}
  puts "### There are #{new_prosecutions.size} new prosecutions"
  new_prosecutions.map! {|p| geocode(p) }

  # Serialise
  ScraperWiki.save_sqlite(['id'], new_prosecutions)

  puts "Done"
end

main()
