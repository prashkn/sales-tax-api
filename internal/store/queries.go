package store

const (
	queryJurisdictionsByZIP = `
		SELECT j.fips_code, j.name, j.type, j.state_fips, j.parent_fips, j.effective_date
		FROM zip_to_jurisdictions z
		JOIN jurisdictions j ON j.fips_code = z.fips_code
		WHERE z.zip_code = $1
		  AND z.expiry_date IS NULL
		ORDER BY z.is_primary DESC`

	queryRateByFIPS = `
		SELECT id, fips_code, rate, rate_type, effective_date, expiry_date, source
		FROM rates
		WHERE fips_code = $1
		  AND expiry_date IS NULL
		  AND rate_type = 'general'
		ORDER BY effective_date DESC
		LIMIT 1`

	queryDataFreshness = `
		SELECT MAX(updated_at) FROM jurisdictions`
)