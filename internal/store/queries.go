package store

import sq "github.com/Masterminds/squirrel"

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func jurisdictionsByZIPQuery(zip string) sq.SelectBuilder {
	return psql.
		Select("j.fips_code", "j.name", "j.type", "j.state_fips", "j.parent_fips", "j.effective_date").
		From("zip_to_jurisdictions z").
		Join("jurisdictions j ON j.fips_code = z.fips_code").
		Where(sq.Eq{"z.zip_code": zip}).
		Where("z.expiry_date IS NULL").
		OrderBy("z.is_primary DESC")
}

func rateByFIPSQuery(fipsCode string) sq.SelectBuilder {
	return psql.
		Select("id", "fips_code", "rate", "rate_type", "effective_date", "expiry_date", "source").
		From("rates").
		Where(sq.Eq{"fips_code": fipsCode}).
		Where("expiry_date IS NULL").
		Where(sq.Eq{"rate_type": "general"}).
		OrderBy("effective_date DESC").
		Limit(1)
}

func ratesByFIPSCodesQuery(fipsCodes []string) sq.SelectBuilder {
	return psql.
		Select("id", "fips_code", "rate", "rate_type", "effective_date", "expiry_date", "source").
		From("rates").
		Where(sq.Eq{"fips_code": fipsCodes}).
		Where("expiry_date IS NULL").
		Where(sq.Eq{"rate_type": "general"}).
		OrderBy("fips_code", "effective_date DESC")
}

// func dataFreshnessQuery() sq.SelectBuilder {
// 	return psql.
// 		Select("MAX(updated_at)").
// 		From("jurisdictions")
// }
