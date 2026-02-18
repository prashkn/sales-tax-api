-- Seed data: sample jurisdictions and rates for testing
-- Covers a few well-known ZIP codes across different states

-- States
INSERT INTO jurisdictions (fips_code, name, type, state_fips, parent_fips, effective_date) VALUES
('06',     'California',       'state',  '06', NULL, '2024-01-01'),
('36',     'New York',         'state',  '36', NULL, '2024-01-01'),
('17',     'Illinois',         'state',  '17', NULL, '2024-01-01'),
('48',     'Texas',            'state',  '48', NULL, '2024-01-01'),
('41',     'Oregon',           'state',  '41', NULL, '2024-01-01');

-- Counties
INSERT INTO jurisdictions (fips_code, name, type, state_fips, parent_fips, effective_date) VALUES
('06037',  'Los Angeles County',  'county', '06', '06', '2024-01-01'),
('36061',  'New York County',     'county', '36', '36', '2024-01-01'),
('17031',  'Cook County',         'county', '17', '17', '2024-01-01'),
('48201',  'Harris County',       'county', '48', '48', '2024-01-01'),
('41051',  'Multnomah County',    'county', '41', '41', '2024-01-01');

-- Cities
INSERT INTO jurisdictions (fips_code, name, type, state_fips, parent_fips, effective_date) VALUES
('0603744000', 'Beverly Hills',    'city', '06', '06037', '2024-01-01'),
('0644000',    'Los Angeles',      'city', '06', '06037', '2024-01-01'),
('3651000',    'New York City',    'city', '36', '36061', '2024-01-01'),
('1714000',    'Chicago',          'city', '17', '17031', '2024-01-01'),
('4835000',    'Houston',          'city', '48', '48201', '2024-01-01'),
('4159000',    'Portland',         'city', '41', '41051', '2024-01-01');

-- Special districts
INSERT INTO jurisdictions (fips_code, name, type, state_fips, parent_fips, effective_date) VALUES
('06037SD01', 'LA Metro Transportation Authority', 'special_district', '06', '06037', '2024-01-01'),
('17031SD01', 'Chicago Transit Authority',         'special_district', '17', '17031', '2024-01-01');

-- State rates
INSERT INTO rates (fips_code, rate, rate_type, effective_date, expiry_date, source) VALUES
('06',     0.07250, 'general', '2024-01-01', NULL, 'state_gov'),
('36',     0.04000, 'general', '2024-01-01', NULL, 'state_gov'),
('17',     0.06250, 'general', '2024-01-01', NULL, 'state_gov'),
('48',     0.06250, 'general', '2024-01-01', NULL, 'state_gov'),
('41',     0.00000, 'general', '2024-01-01', NULL, 'state_gov');

-- County rates
INSERT INTO rates (fips_code, rate, rate_type, effective_date, expiry_date, source) VALUES
('06037',  0.00250, 'general', '2024-01-01', NULL, 'state_gov'),
('36061',  0.04500, 'general', '2024-01-01', NULL, 'state_gov'),
('17031',  0.01750, 'general', '2024-01-01', NULL, 'state_gov'),
('48201',  0.02000, 'general', '2024-01-01', NULL, 'state_gov'),
('41051',  0.00000, 'general', '2024-01-01', NULL, 'state_gov');

-- City rates
INSERT INTO rates (fips_code, rate, rate_type, effective_date, expiry_date, source) VALUES
('0603744000', 0.01250, 'general', '2024-01-01', NULL, 'state_gov'),
('0644000',    0.00000, 'general', '2024-01-01', NULL, 'state_gov'),
('3651000',    0.04500, 'general', '2024-01-01', NULL, 'state_gov'),
('1714000',    0.01250, 'general', '2024-01-01', NULL, 'state_gov'),
('4835000',    0.00000, 'general', '2024-01-01', NULL, 'state_gov'),
('4159000',    0.00000, 'general', '2024-01-01', NULL, 'state_gov');

-- Special district rates
INSERT INTO rates (fips_code, rate, rate_type, effective_date, expiry_date, source) VALUES
('06037SD01', 0.00500, 'general', '2024-01-01', NULL, 'state_gov'),
('17031SD01', 0.01000, 'general', '2024-01-01', NULL, 'state_gov');

-- ZIP code to jurisdiction mappings

-- 90210 (Beverly Hills, CA) -> state + county + city + special district = 7.25 + 0.25 + 1.25 + 0.50 = 9.25%
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('90210', '06',           true,  '2024-01-01', NULL),
('90210', '06037',        true,  '2024-01-01', NULL),
('90210', '0603744000',   true,  '2024-01-01', NULL),
('90210', '06037SD01',    true,  '2024-01-01', NULL);

-- 90001 (Los Angeles, CA) -> state + county + city + special district = 7.25 + 0.25 + 0.00 + 0.50 = 8.00%
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('90001', '06',           true,  '2024-01-01', NULL),
('90001', '06037',        true,  '2024-01-01', NULL),
('90001', '0644000',      true,  '2024-01-01', NULL),
('90001', '06037SD01',    true,  '2024-01-01', NULL);

-- 10001 (Manhattan, NYC) -> state + county + city = 4.00 + 4.50 + 4.50 = 13.00% (one of the highest in the US... but not quite - real rate is 8.875%)
-- Note: NYC is a special case where the city rate effectively replaces county. Simplified here for testing.
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('10001', '36',           true,  '2024-01-01', NULL),
('10001', '36061',        true,  '2024-01-01', NULL),
('10001', '3651000',      true,  '2024-01-01', NULL);

-- 60601 (Chicago, IL) -> state + county + city + special district = 6.25 + 1.75 + 1.25 + 1.00 = 10.25%
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('60601', '17',           true,  '2024-01-01', NULL),
('60601', '17031',        true,  '2024-01-01', NULL),
('60601', '1714000',      true,  '2024-01-01', NULL),
('60601', '17031SD01',    true,  '2024-01-01', NULL);

-- 77001 (Houston, TX) -> state + county + city = 6.25 + 2.00 + 0.00 = 8.25%
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('77001', '48',           true,  '2024-01-01', NULL),
('77001', '48201',        true,  '2024-01-01', NULL),
('77001', '4835000',      true,  '2024-01-01', NULL);

-- 97201 (Portland, OR) -> state + county + city = 0.00 + 0.00 + 0.00 = 0.00% (Oregon has no sales tax)
INSERT INTO zip_to_jurisdictions (zip_code, fips_code, is_primary, effective_date, expiry_date) VALUES
('97201', '41',           true,  '2024-01-01', NULL),
('97201', '41051',        true,  '2024-01-01', NULL),
('97201', '4159000',      true,  '2024-01-01', NULL);
