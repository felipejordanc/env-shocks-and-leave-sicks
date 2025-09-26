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
use bla bla
drop if entity == 16180 |entity == 16437 |entity == 16578 |entity == 16692 |entity == 16693 |entity == 16694 |entity == 16695 |entity == 16696 |entity == 16697 |entity == 16698 |entity == 16699 |entity == 16700 |entity == 16701 |entity == 16702 |entity == 16703 |entity == 27600 |entity == 28120 |entity == 28197 