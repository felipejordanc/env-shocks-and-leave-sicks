  zvgb x x   


local mydir "C:\Users\black\Documents\Plantillas personalizadas de Office\OneDrive_6_7-11-2025_0_100\LT8696_TEST_999_20250710\"
local files : dir "`mydir'" files "*.txt"
local i = 1
foreach f of local files {
	import delimited "`mydir'\`f'", varnames(1) clear
	tostring periodo_renta, replace
	replace periodo_renta = substr(periodo_renta,1,4) + "m" + substr(periodo_renta,5,2)
	gen date = monthly(periodo_renta, "YM")
	format date %tm
	drop if date < tm(2018m1)
	gen year = year(dofm(date))
	keep codigo_cotizante codigo_empleador year
	duplicates drop codigo_cotizante codigo_empleador year, force
	tempfile e_`i'
	save `e_`i''
	local ++i
}
use `e_1'
forvalues y = 2/1000{
	append using `e_`y''
}
duplicates drop codigo_cotizante codigo_empleador year, force
gen suma = 1
collapse (sum) suma, by(codigo_empleador year)
gen fsize = 1
replace fsize = 2 if suma >=50 & suma <= 199
replace fsize = 3 if suma >=200
label define lab 1 "Small" 2 "Medium" 3 "Large"
label value fsize lab
drop suma
save "$usedata/firm_size.dta", replace


