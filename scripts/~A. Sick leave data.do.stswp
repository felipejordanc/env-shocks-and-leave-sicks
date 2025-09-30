*********************************************
* Purpose: Sick leave database
*
* Date: 05/08/2025
*
* Author: Matías Black
*********************************************
clear all

************************************************
*                0. Key Macros                 *
************************************************

*Folder globals

di "current user: `c(username)'"
if "`c(username)'" == "black"{
	global path "C:/Users/black/Documents/GitHub/env-shocks-and-leave-sicks"
}



	global rawdata "$path/remote/data/A_raw"
	global usedata "$path/remote/data/B_intermediate"
	global final "$path/remote/data/C_final"
	global graphs "$path/graphs"
	global tables "$path/tables"
	
************************************************
*       1. Random sample                       *
************************************************
local mydir "C:\Users\black\Documents\Plantillas personalizadas de Office\OneDrive_6_7-11-2025_0_100\LT8696_TEST_999_20250710\"
local files : dir "`mydir'" files "*.txt"
local i = 1
foreach f of local files {
	import delimited "`mydir'\`f'", varnames(1) clear
	rename codigo_cotizante benef_enciptado
	tostring periodo_renta, replace
	replace periodo_renta = substr(periodo_renta,1,4) + "m" + substr(periodo_renta,5,2)
	gen date = monthly(periodo_renta, "YM")
	format date %tm
	drop if date < tm(2018m1)
	drop periodo_renta
	duplicates drop benef_enciptado, force
	keep benef_enciptado
	tempfile e_`i'
	save `e_`i''
	local ++i
}
use `e_1'
forvalues y = 2/1000{
	append using `e_`y''
}
duplicates drop benef_enciptado, force

save "$usedata/benef_sample.dta", replace // This one has to be deleted when making the replication file, it is just a checkpoint
set seed 12345
sample 1
save "$usedata/benef_sample1.dta", replace

************************************************
*         2. Population data cleaning          *
************************************************
foreach y in 2018 2019 2020{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	merge 1:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	gen sex = sexo_cor == "HOMBRE"
	gen carga = titular_carga == "CARGA"
	gen foreign = nacionalidad_cor == "EXTRANJERA"
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado densidad tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}


foreach y in 2022 2023 2024{
	import delimited "$rawdata/population/Base Beneficiarios/Data Poblacion `y'.csv", clear varnames(1) 
	rename código_beneficiario benef_enciptado
	merge 1:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	gen sex = sexo == "Hombre"
	gen carga = titular_carga == "Carga"
	gen foreign = nacionalidad == "Extranjera"
	merge n:1 comuna_beneficiario using "C:\Users\black\Dropbox\GSL\Bases intermedias\codigo_comunas.dta", update nogenerate keep(3)
	rename tramo_fonasa tramo
	rename tipo_asegurado desc_tipo_asegurado
	keep benef_enciptado edad_tramo cod_comuna_pernat tramo desc_tipo_asegurado tramo_renta sex carga foreign
	gen year = `y'
	tempfile b_`y'
	save `b_`y''
}
import delimited "$rawdata/population/Benef_FNS_2021\Benef_FNS_2021.txt", clear varnames(1)
rename id_asegurado benef_enciptado
merge 1:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
merge n:1 comuna using "C:\Users\black\Dropbox\codigo_comunas.dta", update nogenerate keep(3)
rename grupo tramo
rename tipo_trabajador desc_tipo_asegurado
rename tramo_ingreso tramo_renta
gen sex = sexo == "HOMBRE"
gen carga = titular_carga == "CARGA"
gen year = 2021
rename edad edad_tramo
tostring edad_tramo, replace
drop año fec fecha_nacimiento sexo titular_carga id_titular comuna mto_cotiz_mes comuna_beneficiario
tempfile b_2021
save `b_2021'

use `b_2018'
foreach y in 2019 2020 2021 2022 2023 2024{
	append using `b_`y''
}
replace edad_tramo = "0 a 9 años" if edad_tramo == "00 a 02 años" | edad_tramo == "00 a 4 años" | edad_tramo == "03 a 04 años" | edad_tramo == "05 a 09 años" 
replace edad_tramo = "10 a 19 años" if edad_tramo == "10 a 14 años" | edad_tramo == "15 a 19 años" 
replace edad_tramo = "20 a 29 años" if edad_tramo == "20 a 24 años" | edad_tramo == "25 a 29 años"
replace edad_tramo = "30 a 39 años" if edad_tramo == "30 a 34 años" | edad_tramo == "35 a 39 años" 
replace edad_tramo = "40 a 49 años" if edad_tramo == "40 a 44 años" | edad_tramo == "45 a 49 años" 
replace edad_tramo = "50 a 59 años" if edad_tramo == "50 a 54 años" | edad_tramo == "55 a 59 años" 
replace edad_tramo = "60 a 69 años" if edad_tramo == "60 a 64 años" | edad_tramo == "65 a 69 años" 
replace edad_tramo = "70 a 79 años" if edad_tramo == "70 a 74 años" | edad_tramo == "75 a 79 años" 
replace edad_tramo = "80 a más años" if edad_tramo == "80 a 84 años" | edad_tramo == "85 a 89 años" | edad_tramo == "90 a 94 años" | edad_tramo == "95 a 99 años" | edad_tramo == "Más de 99 años" 
replace edad_tramo = "NA" if edad_tramo == "S.I." | edad_tramo == "Sin información" 
replace desc_tipo_asegurado = "Carente" if desc_tipo_asegurado == "CARENTE"
replace desc_tipo_asegurado = "Desconocido" if desc_tipo_asegurado == "DESCONOCIDO"
replace desc_tipo_asegurado = "Pensionado" if desc_tipo_asegurado == "PENSIONADO"
replace desc_tipo_asegurado = "Trabajador Dependiente" if desc_tipo_asegurado == "TRABAJADOR DEPENDIENTE"
replace desc_tipo_asegurado = "Trabajador Independiente" if desc_tipo_asegurado == "TRABAJADOR INDEPENDIENTE"

save "$usedata/Population.dta", replace

************************************************
*         3. Employer data cleaning          *
************************************************
local mydir "C:\Users\black\Documents\Plantillas personalizadas de Office\OneDrive_6_7-11-2025_0_100\LT8696_TEST_999_20250710\"
local files : dir "`mydir'" files "*.txt"
local i = 1
foreach f of local files {
	import delimited "`mydir'\`f'", varnames(1) clear
	rename codigo_cotizante benef_enciptado
	merge n:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)
	tostring periodo_renta, replace
	replace periodo_renta = substr(periodo_renta,1,4) + "m" + substr(periodo_renta,5,2)
	gen date = monthly(periodo_renta, "YM")
	format date %tm
	drop if date < tm(2018m1)
	drop periodo_renta
	tempfile e_`i'
	save `e_`i''
	local ++i
}
use `e_1'
forvalues y = 2/1000{
	append using `e_`y''
}
save "$usedata/Employer.dta", replace
bysort benef_enciptado date: gen n = _n
gen year = year(dofm(date))
bysort benef_enciptado year: egen mode_emp = mode(codigo_empleador)
bysort benef_enciptado date: gen match = codigo_empleador == mode_emp
sort benef_enciptado date match monto_renta_imponible_pesos
collapse(sum) monto_renta_imponible_pesos (max) n (lastnm) codigo_empleador actividad_economica, by(benef_enciptado date)
save "$usedata/Employer.dta", replace
************************************************
*         4. Sick leave data cleaning          *
************************************************
import delimited "$rawdata/sick leaves\T8314.csv", clear varnames(1)
gen date = daily(fecha_emision, "DMY")
format date %td
gen date2 = daily(fecha_desde, "DMY")
format date2 %td
gen year = year(date)
bysort rut_prof_encriptado year: gen obs_id_year = _N
bysort year: egen cutoff = pctile(obs_id_year), p(99)
gen top1pct = obs_id_year >= cutoff
rename encripbi_rut_traba benef_enciptado
gen delta = date - date2
keep if delta <= 7 & delta >= -7
duplicates drop benef_enciptado date, force // Cambiar a keep max
merge n:1 benef_enciptado using "$usedata/benef_sample1.dta", nogenerate keep(3)

keep benef_enciptado date dias_otorgados cod_tipo_licencia top1pct delta
save "$usedata/Sick.dta", replace

************************************************
*         5. Main data cleaning          *
************************************************

clear all
set obs 1
gen date = mdy(1,1,2018)
format date %td
gen enddate = mdy(12,31,2024)
format enddate %td
expand enddate - date + 1
replace date = date[1] + _n +-1
drop enddate
tempfile dates
save `dates'

use "$usedata/benef_sample1.dta", clear
cross using `dates'
sort benef_enciptado date
gen leave = 0

merge 1:1 benef_enciptado date using "$usedata/Sick.dta", nogenerate keep (1 3)
gen enddate = .
replace enddate = date + dias_otorgados - 1 if dias_otorgados < .
bysort benef_enciptado (date): replace enddate = enddate[_n-1] if enddate[_n-1] > date[_n-1] & enddate[_n-1] != . & enddate == .
replace leave = -98 if enddate != . & leave != 1
drop enddate dias_otorgados
*gen year = year(date)
gen ym = mofd(date)
format ym %tm
save "$usedata/Main.dta", replace
use "$usedata/Main.dta", clear














