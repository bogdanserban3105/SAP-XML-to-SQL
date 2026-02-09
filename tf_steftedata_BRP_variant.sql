 /********* Begin Procedure Script ************/ 
   BEGIN 
	  
--Get all required column and respective records until the Verwerkings Date.
	IT_LEGO_MUTATION = 
		select distinct 
				_BIC_ZMUTID as  ZMUTID,
				_BIC_ZVTAID as  ZVTAID,
				_BIC_ZPDEELNID as  ZPDEELNID,
				_BIC_ZVERW_DAT as  ZVERW_DAT,			 
				_BIC_MWYZVLGNR as  MWYZVLGNR,
				_BIC_ZMUTDATE as  ZMUTDATE,
				_BIC_ZMUTJAAR as  ZMUTJAAR,
				case when substring(_BIC_ZMUTDATE,5,4) = '0101'	--this field is used for the BOY snapshot, which is on 
				then _BIC_ZMUTJAAR								--1st of January. Therefore a field is created in which the
				else (_BIC_ZMUTJAAR+1)							--mutation date is increased by 1, which allows us to use the same
						end as BEGIN_JAAR,						--logic as for EOY snapshot but with a different meaning
				_BIC_ZAANSTYP as  ZAANSTYP,
				_BIC_ZVZ_GROEP as  ZVZ_GROEP,
				_BIC_MMUTCOD as  MMUTCOD,
				_BIC_ZPDEELID as  ZPDEELID,
				_BIC_ZEVENTID as  ZEVENTID,
				_BIC_ZVVAANTP as  ZVVAANTP,
				_BIC_ZWERKGID as  ZWERKGID,
				_BIC_ZINDATPOL as  ZINDATPOL,
				_BIC_ZVSTATP as  ZVSTATP,
				_BIC_ZVVSTATP as  ZVVSTATP,
				_BIC_ZVVSTATA as  ZVVSTATA,
				_BIC_ZVSTATA as  ZVSTATA,
				_BIC_ZVFINSTA as  ZVFINSTA,
				_BIC_ZBSN1 as  ZBSN1,
				_BIC_ZGESL as  ZGESL,
				case when _BIC_ZGEBDAT = '00000000'
				then ''
				else _BIC_ZGEBDAT end as ZGEBDAT,	--simplify checks on filled birthdates 
				_BIC_ZSTEDAT as ZSTEDAT,
				_BIC_ZTYPE as  ZTYPE,
				_BIC_ZVZBSN as  ZVZBSN,
				_BIC_ZVZGESL as  ZVZGESL,
				_BIC_ZVZGEBDAT as  ZVZGEBDAT,
				_BIC_ZVZSTEDAT as  ZVZSTEDAT,
				_BIC_ZTGZ_BDR as  ZTGZ_BDR,
				_BIC_ZVERBDR as  ZVERBDR,
				_BIC_ZLTSAL as ZLTSAL,
				_BIC_ZLTSALDT as ZLTSALDT,
				_BIC_ZBSSALAM as ZBSSALAM,
				_BIC_MBETTCP as MBETTCP ,
				_BIC_MBETMCP as MBETMCP,
				case when 
				(_BIC_ZPRVMUTID <> '')
				then 'X'
				else ''
				end as CORRECTION_APPLIED,			--simplify identification of 62000 mutations
				_BIC_ZPENREGID as ZPENREGID		--cov slice key, still needed for now
		from "NNROOT.Z00_Applications.VOD.BaseViews/CV_AZ21_O3952"
		where (_BIC_ZPRVMUTID = '') -- Do not include 62000 policies!
			and _BIC_ZVERW_DAT <= :IP_VERW_DATUM
			and _BIC_ZMUTJAAR <= :IP_MUTJAAR_TOT;

--------------------------------------------------------------------------------------------------------------------
----------------------------------------------------ReUsed Policies-------------------------------------------------
	
--prepare data and structure for calling a tablefunction
--BIJAFKANT will tell us if on policy level all coverages have ended with a mutation or not
--62000 mutations must be ignored, they are not 'really' part of the policy
IT_REUSED_POLICY_STRUCTURE =
	select 
		ZPDEELID,
		ZVERW_DAT,				 
		MWYZVLGNR,				
		ZMUTDATE,
		ZGEBDAT,
		max(ZTYPE) as BIJAFKANT	
	from :IT_LEGO_MUTATION	
	where CORRECTION_APPLIED = ''
	group by
		ZMUTID,
		ZPDEELID,
		ZVERW_DAT,				 
		MWYZVLGNR,				
		ZMUTDATE,
		ZGEBDAT
	;	
	
--call tablefunction, this will return only those policies that are reused
--per mutation we received REUSED_ID, which will become the new ZPDEELID
IT_REUSED_POLICY_IDS = 
	select
		ZPDEELID,
		ZVERW_DAT,				 
		MWYZVLGNR,				
		ZMUTDATE,
		REUSED_ID
	from "_SYS_BIC"."NNROOT.Z00_Applications.VOD.ReportBase.LEGO::tf_reusedpolicies" (:IT_REUSED_POLICY_STRUCTURE)
	;	

--We join back the REUSED_ID with the entire dataset
--We only use the REUSED_ID if there is more than one per unique policy ID, thereby filtering out
--the previously identified cases in which the policy turned out not to be reused
IT_REUSED_POLICY_PRE =		
	select 	
			allmut.ZMUTID,
			allmut.ZVTAID,
			allmut.ZPDEELNID,
			allmut.ZVERW_DAT,
			allmut.MWYZVLGNR,
			allmut.ZMUTDATE,
			allmut.ZMUTJAAR,
			allmut.BEGIN_JAAR,			
			allmut.ZAANSTYP,
			allmut.ZVZ_GROEP,
			allmut.MMUTCOD,
			case 
				when (reused.REUSED_ID is not null)
				then reused.REUSED_ID
				else allmut.ZPDEELID
			end as ZPDEELID, 
			allmut.ZEVENTID,
			allmut.ZVVAANTP,
			allmut.ZWERKGID,
			allmut.ZINDATPOL,
			allmut.ZVSTATP,
			allmut.ZVVSTATP,
			allmut.ZVVSTATA,
			allmut.ZVSTATA,
			allmut.ZVFINSTA,
			allmut.ZBSN1,
			allmut.ZGESL,
			allmut.ZGEBDAT,		 
			allmut.ZSTEDAT,
			allmut.ZTYPE,
			allmut.ZVZBSN,
			allmut.ZVZGESL,
			allmut.ZVZGEBDAT,
			allmut.ZVZSTEDAT,
			allmut.ZTGZ_BDR,
			allmut.ZVERBDR,
			allmut.ZLTSAL,
			allmut.ZLTSALDT,
			allmut.ZBSSALAM,
			allmut.MBETTCP,
			allmut.MBETMCP,
			allmut.CORRECTION_APPLIED,	
			allmut.ZPENREGID			
	from :IT_LEGO_MUTATION as allmut			--all mutations of all policies, unmarked for reused 
	left join :IT_REUSED_POLICY_IDS as reused	--all mutations of the reused policies, marked for reused
		on allmut.ZPDEELID = reused.ZPDEELID
		and allmut.ZVERW_DAT = reused.ZVERW_DAT
		and allmut.MWYZVLGNR = reused.MWYZVLGNR
		and allmut.ZMUTDATE = reused.ZMUTDATE
;	

IT_REUSED_POLICY =		
	select 	
			ZMUTID,
			ZVTAID,
			ZPDEELNID,
			ZVERW_DAT,
			MWYZVLGNR,
			ZMUTDATE,
			ZMUTJAAR,
			BEGIN_JAAR,			
			ZAANSTYP,
			ZVZ_GROEP,
			MMUTCOD,
			ZPDEELID, 
			ZEVENTID,
			ZVVAANTP,
			ZWERKGID,
			ZINDATPOL,
			ZVSTATP,
			ZVVSTATP,
			ZVVSTATA,
			ZVSTATA,
			ZVFINSTA,
			ZBSN1,
			ZGESL,
			ZGEBDAT,		 
			ZSTEDAT,
			ZTYPE,
			ZVZBSN,
			ZVZGESL,
			ZVZGEBDAT,
			ZVZSTEDAT,
			ZTGZ_BDR,
			SUM(ZVERBDR) as ZVERBDR,
			ZLTSAL,
			ZLTSALDT,
			ZBSSALAM,
			MBETTCP,
			MBETMCP,
			CORRECTION_APPLIED				
	from :IT_REUSED_POLICY_PRE
	group by	
			ZMUTID,
			ZVTAID,
			ZPDEELNID,
			ZVERW_DAT,
			MWYZVLGNR,
			ZMUTDATE,
			ZMUTJAAR,
			BEGIN_JAAR,			
			ZAANSTYP,
			ZVZ_GROEP,
			MMUTCOD,
			ZPDEELID, 
			ZEVENTID,
			ZVVAANTP,
			ZWERKGID,
			ZINDATPOL,
			ZVSTATP,
			ZVVSTATP,
			ZVVSTATA,
			ZVSTATA,
			ZVFINSTA,
			ZBSN1,
			ZGESL,
			ZGEBDAT,		 
			ZSTEDAT,
			ZTYPE,
			ZVZBSN,
			ZVZGESL,
			ZVZGEBDAT,
			ZVZSTEDAT,
			ZTGZ_BDR,
			--no ZVERBDR
			ZLTSAL,
			ZLTSALDT,
			ZBSSALAM,
			MBETTCP,
			MBETMCP,
			CORRECTION_APPLIED	
			--no ZPENREGID			
;		
		
----------------------------------------------------Previous information-------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
	 
--Select mutations on coverage level and find the next mutation per coverage, 
--this next mutation will get the info from the current mutation, also select the fields which are relevant for retrieval
--the 62000 mutations must stay in this table for the 62000 logic later, but next mutation determination must ignore them (so use in partition)
	  IT_COV_PREV_MUT =
		 SELECT 
				ZMUTID,
				ZPDEELID,
		        ZVVAANTP,	
		        ZAANSTYP,	        		
				ZVERW_DAT,
				MWYZVLGNR,
				ZMUTDATE,
				ZEVENTID,
				ZVSTATP,
				ZVVSTATP,
				ZTGZ_BDR,
				ZVERBDR,
				ZVSTATA,
				ZVVSTATA,
				ZTYPE,
				ZBSN1,
				ZSTEDAT,
				-- CHANGE --------- ADD ZVZSTEDAT			
				ZVZSTEDAT,
				CORRECTION_APPLIED,
				LEAD(ZMUTID) over (partition by ZPDEELID, ZVVAANTP, CORRECTION_APPLIED  
	  								 order by ZVERW_DAT, MWYZVLGNR, ZMUTDATE) as NEXT_MUT_COVERAGE
	  	 from :IT_REUSED_POLICY 	
	  	 group by 
				ZMUTID,
				ZPDEELID,	
		        ZVVAANTP,	
		        ZAANSTYP,	        		
				ZVERW_DAT,
				MWYZVLGNR,
				ZMUTDATE,
				ZEVENTID,
				ZVSTATP,
				ZVVSTATP,
				ZTGZ_BDR,
				ZVERBDR,
				ZVSTATA,
				ZVVSTATA,
				ZTYPE,
				ZBSN1,
				ZSTEDAT,
				------CHANGE-------ADD ZVZSTEDAT
				ZVZSTEDAT,
				CORRECTION_APPLIED				   
	  	 ; 

--retrieve previous information
	  IT_LEGO_RELEVANT_TRANSACTIONS =
	     SELECT allmut.*,
	     		cov_prev.ZVSTATP as ZVSTATPV,
	     		cov_prev.ZVVSTATP as ZVVSTATPV,
	     		cov_prev.ZTGZ_BDR as ZTGZ_BDRO,
	     		cov_prev.ZVSTATA as ZVSTATAV,
	     		cov_prev.ZVVSTATA as ZVVSTATAV,
				cov_prev.ZVERBDR as ZVERBDRO
	     from :IT_REUSED_POLICY as allmut
	     left join :IT_COV_PREV_MUT as cov_prev
	      	on allmut.ZMUTID    = cov_prev.NEXT_MUT_COVERAGE 
	     	and allmut.ZVVAANTP  = cov_prev.ZVVAANTP 
			-- where allmut.CORRECTION_APPLIED = ''	    --CHANGE Now all correction applied are involved. (not valid for 62000 records!). This is needed cause all deathcases should be taken into account.
			;
	
/**********Latest personal information**************/
	
--per VOD requirements, we want to display the latest available personal information in the reports
	
--First we will prepare data in four steps. For both the first and second insured seperately, we want to find:
----1. All mutations where either birthdate, gender, or BSN is filled --> the GBB
----2. ALl mutations with death date filled. Note that only unreversed deaths are in the data at this point, so no need to check on E001/R001/E002/R002
	
--First insured is determined on policy level
--First insured, death. Select only decease mutations
	IT_DEATH_PREP_FIRST =
		select distinct
			ZPDEELID as GROUP_FIELD1,
			'' as GROUP_FIELD2,
			'First' as GROUP_FIELD3,	--First as in first insured
			'Death' as GROUP_FIELD4,	--Death as in for the death data 
			
			ZVERW_DAT as SORT_FIELD1,
			MWYZVLGNR as SORT_FIELD2,
			ZMUTDATE as SORT_FIELD3,
			
			'' as BIRTHDATE,
			'' as GENDER,
			'' as BSN,
			ZSTEDAT as DEATHDATE,
			
			'LEGO' as VTA,
			ZEVENTID as MUTATIONEVENT
		from :IT_LEGO_RELEVANT_TRANSACTIONS
		where 
			ZEVENTID in ('E001','R001')
	;

--First insured, GBB
	IT_GBB_PREP_FIRST = 
		select distinct
			ZPDEELID as GROUP_FIELD1,
			'' as GROUP_FIELD2,
			'First' as GROUP_FIELD3,	--First as in first insured
			'GBB' as GROUP_FIELD4,		--GBB as in for the personal data
			
			ZVERW_DAT as SORT_FIELD1,
			MWYZVLGNR as SORT_FIELD2,
			ZMUTDATE as SORT_FIELD3,
			
			ZGEBDAT as BIRTHDATE,
			ZGESL as GENDER,
			ZBSN1 as BSN,
			'' as DEATHDATE,
			
			'LEGO' as VTA,
			'' as MUTATIONEVENT
		from :IT_LEGO_RELEVANT_TRANSACTIONS
	;
	
--Second insured, death
	IT_DEATH_PREP_SECOND = 
		select distinct
			ZPDEELID as GROUP_FIELD1,
			'' as GROUP_FIELD2,
			'Second' as GROUP_FIELD3,	--2 as in second insured
			'Death' as GROUP_FIELD4,
			
			ZVERW_DAT as SORT_FIELD1,
			MWYZVLGNR as SORT_FIELD2,
			ZMUTDATE as SORT_FIELD3,
			
			'' as BIRTHDATE,
			'' as GENDER,
			'' as BSN,
			ZVZSTEDAT as DEATHDATE, 
			
			'LEGO' as VTA,
			ZEVENTID as MUTATIONEVENT
		from :IT_LEGO_RELEVANT_TRANSACTIONS	
		where 
			ZEVENTID in ('E002','R002')
			and ZAANSTYP in  ('008', '015')
	;
	
--Second insured, GBB
	IT_GBB_PREP_SECOND = 	
		select distinct
			ZPDEELID as GROUP_FIELD1,
			'' as GROUP_FIELD2,
			'Second' as GROUP_FIELD3,	--2 as in second insured
			'GBB' as GROUP_FIELD4, 
			
			ZVERW_DAT as SORT_FIELD1,
			MWYZVLGNR as SORT_FIELD2,
			ZMUTDATE as SORT_FIELD3,
			
			ZVZGEBDAT as BIRTHDATE,
			ZVZGESL as GENDER,
			ZVZBSN as BSN,
			'' as DEATHDATE,
			
			'LEGO' as VTA,
			'' as MUTATIONEVENT
		from :IT_LEGO_RELEVANT_TRANSACTIONS
		where 
			ZVZGEBDAT <> '00000000'	
	;

--combine the four previous steps so that we can feed them to the external tablefunction in one go 
	IT_ALLPERSONAL =
		select * from :IT_DEATH_PREP_FIRST 
		
		union all
		
		select * from :IT_GBB_PREP_FIRST
		
		union all
		
		select * from :IT_DEATH_PREP_SECOND 
		
		union all
		
		select * from :IT_GBB_PREP_SECOND 
	;	

--Next, we pass this table to a table function which will return the latest personal information and latest death date like magic.
	IT_GBB =
		select 
			GROUP_FIELD1 as ZPDEELID,
			--GROUP_FIELD2,
			GROUP_FIELD3 as ROLEKEY,
			GROUP_FIELD4 as PURPOSE,
			--SORT_FIELD1,
			--SORT_FIELD2,
			--SORT_FIELD3,
			BIRTHDATE,		
			GENDER,
			BSN,
			DEATHDATE,
			--VTA
			MUTATIONEVENT as ZEVENTID
		from "_SYS_BIC"."NNROOT.Z00_Applications.VOD.ReportBase.Common::tf_latest_personal_information"(:IT_ALLPERSONAL)
	;
	
	
    if(:IP_CORRECTION_APPLIED = 'X')		--We include 62000 mutations
	then
	
--Select only the 62000 mutations and find the latest one per policy		
		IT_62000 =
	 	 select ZPDEELID,
	  		 	ZVERW_DAT,
	  		 	MWYZVLGNR, 
	  		 	ZMUTDATE,
		        ZEVENTID,
		 		ZTYPE,
		 		ZSTEDAT,
		 		--------------CHANGE- include zvstedat
				ZVZSTEDAT,
				--------------CHANGE- include ZAANSTYP TO BE ABLE TO SELECT 007 FOR FIRST INSURED.
				ZAANSTYP,
		 		ZVSTATA,
		 		ZVVSTATA,
		 		ZBSN1,
			 	ZVSTATP,
	         	ZVVSTATP,
	         	ZTGZ_BDR,
	         	ZVERBDR,
				rank() over (partition by ZPDEELID
		  	   					 order by ZVERW_DAT desc, MWYZVLGNR desc, ZMUTDATE desc) as death_rank 
		  from :IT_COV_PREV_MUT
	      where CORRECTION_APPLIED = 'X';
	
-----------------------CHANGE, IT_6200_ACTIVE NOT NEEDED FOR BRP--------------------------------------------------------------
--Restrict to those where the 62000 policies are still active   
--Additionally, we perform the check if and only if the BSN number in the 62000 policies is the same as in the 'normal' policy
--it belongs to. Note that not all policies have BSN number filled, but this check is necessary to be 100% certain the 
--62000 mutations belong to the policy in which we've placed them in the VOD entities.      
	--- CHANGE---- NOT USED
	  /*
	    IT_62000_ACTIVE =
		  select a.*
			from :IT_62000 a
			inner join :IT_GBB b
			 on a.ZPDEELID = b.ZPDEELID
			 and b.ROLEKEY = 'First'
			 and b.PURPOSE = 'GBB' 
			and a.ZBSN1     = b.BSN
			----------------CHANGE, NO FILTER ON RANK NEEDED, ALL BRP RECORDS ARE TAKEN INTO ACCOUNT
			--where a.death_rank = 1
-------------CHANGE CHANGE CHANGE
		  and a.ZTYPE in ('510_ONLY','BOTH')
	--and a.ZTYPE in ('508_ONLY','510_ONLY','BOTH')
			  and a.ZVVSTATA in (8,9)
			  and a.ZBSN1 <> 0;
			  */
			  
			  
	--		  select 'IT_62000_ACTIVE', * from :IT_62000_ACTIVE;
------------------------- END CHANGE-----------------------------------------------------------------------------------
--END IF;
-- END
	
--We take the latest, unreversed death mutation from 'normal' mutations
--If there exists 62000 mutations that meet the earlier defined criteria and there exists no 
--unreversed death mutation for the participant E001 within the same policy, then it means we 
--have identified an extra death case and collect the policy ID and death date for this policy as well      
	    IT_LEGO_DEATH =
	      select distinct * from (
	      select a.ZPDEELID,
		         a.ZSTEDAT,
		         'X' as CORRECTION_APPLIED
	      from :IT_62000 a     --- CHANGE(d) FROM TABLE IT_62000_ACTIVE
	      left join :IT_GBB b
	       on a.ZPDEELID = b.ZPDEELID
	      and b.ROLEKEY = 'First'
	      and b.PURPOSE = 'Death'
		  and b.ZEVENTID = 'E001' 	--latest decease mut must be unreversed 
		 where A.ZEVENTID = 'B001' ------CHANGE, ONLY BRP DECEASES FIRST INSURED
		  group by b.ZPDEELID, a.ZPDEELID, a.ZSTEDAT
		  having b.ZPDEELID is null
		  ------CHANGE, ONLY FIRST DECEASES
	      
	      union all
	      
	      select ZPDEELID,
		         DEATHDATE as ZSTEDAT,
		         '' as CORRECTION_APPLIED
		  from :IT_GBB  
	       where 1 = 1
		      and ROLEKEY = 'First'
		      and PURPOSE = 'Death'
			  and ZEVENTID = 'E001' 	--latest decease mut must be unreversed 
		        
	         );
			 
	 -----------------CHANGE FOR BRP DEATH SECOND INSURED------------------------------
	 --We take the latest, unreversed death mutation from 'normal' mutations
--If there exists 62000 mutations that meet the earlier defined criteria and there exists no 
--unreversed death mutation for the participant E002 within the same policy, then it means we 
--have identified an extra death case and collect the policy ID and death date for this policy as well      
	    IT_LEGO_DEATH2 =
	      select distinct * from (
	      select a.ZPDEELID,
		         a.ZVZSTEDAT,
		         'X' as CORRECTION_APPLIED
	      from :IT_62000 a     --- CHANGE(d) FROM TABLE IT_62000_ACTIVE
	      left join :IT_GBB b
	       on a.ZPDEELID = b.ZPDEELID
	      and b.ROLEKEY = 'Second'
	      and b.PURPOSE = 'Death'
		  and b.ZEVENTID = 'E002' 	--latest decease mut must be unreversed 
		  ------CHANGE, ONLY FIRST DECEASES
		 where A.ZEVENTID = 'B002'
		-- AND A.ZAANSTYP IN ('008', '015') 
		 and A.ZTYPE = '508_ONLY'   -- only for verzorgde: coverage continues when ztype = 'BOTH'
		  group by b.ZPDEELID, a.ZPDEELID, a.ZVZSTEDAT
		  having b.ZPDEELID is null
	      
	      union all
	      
	      select ZPDEELID,
		         DEATHDATE as ZVZSTEDAT,
		         '' as CORRECTION_APPLIED
		  from :IT_GBB  
	       where 1 = 1
		      and ROLEKEY = 'Second'
		      and PURPOSE = 'Death'
			  and ZEVENTID = 'E002' 	--latest decease mut must be unreversed 
		        
	         );
	         
	         
	 select 'IT_LEGO_DEATH2', * from :IT_LEGO_DEATH2 ;
	       
	    
	    
--------------END CHANGE    
	    
	    
	    
--Previously we already identified the previous mutation. However, for these 62000 deceases, we need to do it separately
--find the latest mutation respective to the latest 62000 mutation
	
--  ONLY FOR FIRST INSURED
	  IT_62000_PREV =	    
		select a.ZPDEELID,
		  	   a.ZVERW_DAT,
		  	   a.MWYZVLGNR, 
		  	   a.ZMUTDATE,
		  	   a.ZVSTATP,
		  	   a.ZVSTATA,
		  	   a.ZVVSTATP,
		       a.ZVVSTATA,
		       a.ZTGZ_BDR,
		       a.ZVERBDR,
			   '' as CORRECTION_APPLIED
		  from :IT_COV_PREV_MUT A
---------FCHANGE---------------
--	  inner join :IT_62000_ACTIVE B  ------CHANGE
	  		inner join :IT_62000 B	  
		   on a.ZPDEELID = b.ZPDEELID
		  and a.ZAANSTYP = '007'
		  and a.CORRECTION_APPLIED = ''
		  and a.ZMUTDATE <= b.ZSTEDAT
		  	  ------CHANGE, verw_dat and B001, ONLY FIRST DECEASES
		 and a.ZVERW_DAT <= B.ZVERW_DAT
		 where B.ZEVENTID = 'B001';    --CHANGE
	
	  IT_62000_PREV_RANK =
	    select *,
		       rank() over (partition by ZPDEELID
		    	    			order by ZVERW_DAT desc, MWYZVLGNR desc, ZMUTDATE desc) as prev_rank
	     from :IT_62000_PREV;
	
--Retrieve the status and amounts		
	  IT_62000_PREV_INFO=
		select distinct ZPDEELID,
		       ZVSTATP as ZVSTATPV,
	           ZVVSTATP as ZVVSTATPV,
	           ZTGZ_BDR as ZTGZ_BDO,
	           ZVERBDR as ZVERBDRO,
	           ZVSTATA as ZVSTATAV,
	           ZVVSTATA as ZVVSTATAV
		from :IT_62000_PREV_RANK 
		where prev_rank = 1;     
	
-- CHANGE FOR SECOND INSURED-----------------------------------------------------------------
	  IT_62000_PREV2 =	    
		select a.ZPDEELID,
		  	   a.ZVERW_DAT,
		  	   a.MWYZVLGNR, 
		  	   a.ZMUTDATE,
		  	   a.ZVSTATP,
		  	   a.ZVSTATA,
		  	   a.ZVVSTATP,
		       a.ZVVSTATA,
		       a.ZTGZ_BDR,
		       a.ZVERBDR,
			   '' as CORRECTION_APPLIED
		  from :IT_COV_PREV_MUT A
---------CHANGE---------------
--	  inner join :IT_62000_ACTIVE B
	  		inner join :IT_62000 B	  
		   on a.ZPDEELID = b.ZPDEELID
		  and a.ZAANSTYP IN ('008', '015')
		  and a.CORRECTION_APPLIED = ''
		  and a.ZMUTDATE <= b.ZVZSTEDAT
		  	  ------CHANGE, ONLY SECOND DECEASES, AND VERW_DAT 
		 and a.ZVERW_DAT <= B.ZVERW_DAT
		 where B.ZEVENTID = 'B002';
	
	select 'IT_62000_PREV2',* from :IT_62000_PREV2;
	
	  IT_62000_PREV_RANK2 =
	    select *,
		       rank() over (partition by ZPDEELID
		    	    			order by ZVERW_DAT desc, MWYZVLGNR desc, ZMUTDATE desc) as prev_rank
	     from :IT_62000_PREV2;
	SELECT 'IT_62000_PREV_RANK2', * FROM :IT_62000_PREV_RANK2;
	
	
	
	
--Retrieve the status and amounts		
	  IT_62000_PREV_INFO2=
		select distinct ZPDEELID,
		       ZVSTATP as ZVSTATPV,
	           ZVVSTATP as ZVVSTATPV,
	           ZTGZ_BDR as ZTGZ_BDO,
	           ZVERBDR as ZVERBDRO,
	           ZVSTATA as ZVSTATAV,
	           ZVVSTATA as ZVVSTATAV
		from :IT_62000_PREV_RANK2 
		where prev_rank = 1;     
	
	SELECT 'IT_62000_PREV_INFO2', * FROM :IT_62000_PREV_INFO2;
	
	
	
	
	
	
	    else --Do NOT include 62000. We still need to do the reversal check 
	    
--verify that latest decease mutation is not cancelled     
	    IT_LEGO_DEATH =
	      	select
	       		ZPDEELID,
		        DEATHDATE as ZSTEDAT,
		        '' as CORRECTION_APPLIED
		  	from :IT_GBB  
	       	where 1 = 1
		      	and ROLEKEY = 'First'
		      	and PURPOSE = 'Death'
			  	and ZEVENTID = 'E001' 	--latest decease mut must be unreversed     
		;
	
	  --A dummy table so that the internal table exists regardless of whether 62000 mutations are considered or not
	  IT_62000_PREV_INFO =
	  	select distinct ZPDEELID,
		       '' as ZVSTATPV,
	           '' as ZVVSTATPV,
	           '0' as ZTGZ_BDO,
	           '0' as ZVERBDRO,
	           '' as ZVSTATAV,
	           '' as ZVVSTATAV
		from :IT_LEGO_DEATH;

	  end if;

--finalize personal information by combining the GBB and the 62000 deceases
--In addition, put all this information on a single row to make joining easier later  
--Add the most recently available personal information and death date for both first and second insured
	  IT_GBB_MUTATION =
	    select distinct
	    	   snapshot.ZPDEELID, 
	           first_decease.ZSTEDAT,					
		       first_decease.CORRECTION_APPLIED,           
	           first_gbb.GENDER as ZGESL,
	           first_gbb.BSN as ZBSN1, 
	           first_gbb.BIRTHDATE as ZGEBDAT,
		       second_decease.ZVZSTEDAT as ZVZSTEDAT,
		       second_decease.CORRECTION_APPLIED as CORRECTION_APPLIED2,    -- CHANGE, ADDED CORRECTION_APPLIED2
	           second_gbb.GENDER as ZVZGESL,
	           second_gbb.BSN as ZVZBSN, 
	           second_gbb.BIRTHDATE as ZVZGEBDAT       
	    from :IT_LEGO_RELEVANT_TRANSACTIONS as snapshot
	    left join :IT_LEGO_DEATH as first_decease
	        on snapshot.ZPDEELID = first_decease.ZPDEELID 
	    left join :IT_GBB as first_gbb
	        on snapshot.ZPDEELID = first_gbb.ZPDEELID
	       and first_gbb.ROLEKEY = 'First'
	       and first_gbb.PURPOSE = 'GBB'
	       -----------------------CHANGE
	    --left join :IT_GBB as second_decease
	      --  on snapshot.ZPDEELID = second_decease.ZPDEELID
	     --  and second_decease.ROLEKEY = 'Second'
	           -- and second_decease.PURPOSE = 'Death'
	      -- and second_decease.ZEVENTID = 'E002'	--last mutation must be unreversed second insured decease
		    left join :IT_LEGO_DEATH2 as second_decease
	        on snapshot.ZPDEELID = second_decease.ZPDEELID 
	       ---------------------------- END CHANGE
	    left join :IT_GBB as second_gbb
	        on snapshot.ZPDEELID = second_gbb.ZPDEELID
	       and second_gbb.ROLEKEY = 'Second'
	       and second_gbb.PURPOSE = 'GBB' 
	       --AND snapshot.ZPDEELID = SECOND_DECEASE.ZPDEELID       -- ONLY VERZORGDE GBB WHEN SECOND DECEASE IS AVAILABLE
	; 
	
--------------------------------------------------------------------------------------------------------------------
----------------------------------------Creation of EOY/BOY snapshots-----------------------------------------------
	
---CHANGE---------------------
-- First define correct dataset. When Deathcase for E001 found, exclude correction_applied = 'X' from relevant transactions cause BRP deathcase
-- must be excluded then. when it_LEGO_DEATH correction_applied = X, than there is a viable Deathcase.

		
	IT_LEGO_RELEVANT_TRANS_DEL1 = 
	select * from :IT_LEGO_RELEVANT_TRANSACTIONS
	where correction_applied = ''  
	union
-- ONLY FIRST INSURED 
	select a.* from :IT_LEGO_RELEVANT_TRANSACTIONS a
	inner join :it_lego_death  b
	on a.zpdeelid = b.zpdeelid
	where a.correction_applied = 'X'
	and a.zeventid = 'B001'
	and a.zstedat = b.zstedat
	and a.ztype = '508_ONLY'
	and b.correction_applied = 'X'
	union
-- SECOND INSURED
	select a.* from :IT_LEGO_RELEVANT_TRANSACTIONS a
	inner join :it_lego_death2  b
	on a.zpdeelid = b.zpdeelid
	where a.correction_applied = 'X'
	and a.zeventid = 'B002'
	and a.zvzstedat = b.zvzstedat
	and a.ztype = '508_ONLY'
	and b.correction_applied = 'X'
	;
	
	select 'IT_LEGO_RELEVANT_TRANS_DEL1', * from :IT_LEGO_RELEVANT_TRANS_DEL1
		order by zvvaantp, zverw_dat, zmutdate;
	
	IT_DEATH_CASE = SELECT * FROM :IT_LEGO_RELEVANT_TRANS_DEL1
	WHERE ZEVENTID in ('B001', 'B002')
	AND ZTYPE = '508_ONLY'
	;
	
	SELECT 'IT_DEATH_CASE', * FROM :IT_DEATH_CASE;
	
	
	
-- Define all mutations from coverage without deathcase
	IT_LEGO_RELEVANT_TRANSACTIONSa =
	select * from  :IT_LEGO_RELEVANT_TRANS_DEL1 a
	where not exists (
	select 1 from :IT_DEATH_CASE b
	       where a.zpdeelid = b.zpdeelid
	       and a.ZVVAANTP = b.ZVVAANTP)
	        union
-- Select from coverage with deathcase all mutations with zverw_dat till and including deathdate      
	select * from  :IT_LEGO_RELEVANT_TRANS_DEL1 a
	where  exists (
	select 1 from :IT_DEATH_CASE b
	       where a.zpdeelid = b.zpdeelid
	       and a.ZVVAANTP = b.ZVVAANTP
	       and a.zverw_dat <= b.zverw_dat);
	
	select 'IT_LEGO_RELEVANT_TRANSACTIONSa', * from :IT_LEGO_RELEVANT_TRANSACTIONSa
		order by zvvaantp, zverw_dat, zmutdate;


	

-----------------------------------END CHANGE------------------------------------------------------

--Identify the year of first mutation for each policy, it can be used as reference when identifying years.
  IT_LEGO_POLICY_START_DATE =
    select ZPDEELID,
           ZAANSTYP,
           ZVZ_GROEP,
           ZVVAANTP,
	       min(ZMUTJAAR) as policy_start_date
	from :IT_LEGO_RELEVANT_TRANSACTIONSa           -- CHANGE
	group by ZPDEELID,ZAANSTYP,ZVZ_GROEP,ZVVAANTP;


--Identify all relevant years for reporting, so from policy inception to the input parameter
--We remove invalid years later
  IT_LEGO_POLICY_PER_YEAR =
    select distinct 
           b.ZPDEELID,
           b.ZAANSTYP,
           b.ZVZ_GROEP,
           b.ZVVAANTP, 
           a.year as RAPPORTAGE_MUTATIE_JAAR
	from _sys_bi.m_time_dimension_year a
	left outer join :IT_LEGO_POLICY_START_DATE b
	    on a.year >= b.policy_start_date
	   and a.year <= :IP_MUTJAAR_TOT
	 where b.ZPDEELID is not null;	

--Identify correct sequence of years for each coverage type
--The purpose is unclear amongst the current developers and was also unclear for the previous developer working on LEGO
-- EXPLANATION (changed comments 20251104)): This logic takes care latest zverw_dat  will always win.
  IT_LEGO_POLICY_PER_YEAR_SEQ =
    select ZPDEELID,
           ZAANSTYP,
           ZVZ_GROEP,
           ZVVAANTP,
           ZMUTJAAR,
           BEGIN_JAAR,
           ZVERW_DAT,
           MWYZVLGNR,
           ZMUTDATE,
           ZTYPE,
           ZEVENTID,
           min(ZMUTJAAR) over (partition by ZPDEELID, ZVVAANTP 
       							   order by ZVERW_DAT desc, MWYZVLGNR desc, ZMUTDATE  desc 
       								      rows unbounded preceding) as CORRECT_MUTJAAR,
       	   min(BEGIN_JAAR) over (partition by ZPDEELID, ZVVAANTP
       								 order by ZVERW_DAT desc, MWYZVLGNR desc, ZMUTDATE  desc 
       								      rows unbounded preceding) as CORRECT_BEGIN_JAAR						      
       								      
	from :IT_LEGO_RELEVANT_TRANSACTIONSa;

--both begin of year and end of year.
  IT_LEGO_PER_YEAR_DIRECT_MATCH =
	select distinct
           a.ZPDEELID,
           a.ZAANSTYP,
           a.ZVZ_GROEP,
           a.ZVVAANTP,
           a.RAPPORTAGE_MUTATIE_JAAR,
           'EoY Snapshot' as REPORT_TYPE,
           first_value(b.ZTYPE)     over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as ZTYPE,
           first_value(b.ZEVENTID)  over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as ZEVENTID,
           first_value(b.ZVERW_DAT) over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as zverw_date_for_link,
           first_value(b.MWYZVLGNR) over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as MWYZVLGNR_for_link,
           first_value(b.ZMUTDATE)  over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as zmutdate_for_link
   from :IT_LEGO_POLICY_PER_YEAR as a
    left join :IT_LEGO_POLICY_PER_YEAR_SEQ as b
       on a.ZPDEELID  = b.ZPDEELID
      and a.ZVVAANTP  = b.ZVVAANTP
      and a.RAPPORTAGE_MUTATIE_JAAR = b.ZMUTJAAR
      and a.RAPPORTAGE_MUTATIE_JAAR = b.CORRECT_MUTJAAR
  
  union all
  
  select distinct
           a.ZPDEELID,
           a.ZAANSTYP,
           a.ZVZ_GROEP,
           a.ZVVAANTP,
           a.RAPPORTAGE_MUTATIE_JAAR,
           'BoY Snapshot' as REPORT_TYPE,
           first_value(b.ZTYPE)     over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as ZTYPE,
           first_value(b.ZEVENTID)  over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as ZEVENTID,
           first_value(b.ZVERW_DAT) over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as zverw_date_for_link,
           first_value(b.MWYZVLGNR) over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as MWYZVLGNR_for_link,
           first_value(b.ZMUTDATE)  over (partition by b.ZPDEELID, b.ZVVAANTP, RAPPORTAGE_MUTATIE_JAAR
                                              order by b.ZVERW_DAT desc, b.MWYZVLGNR desc, b.ZMUTDATE desc) as zmutdate_for_link
   from :IT_LEGO_POLICY_PER_YEAR as a
    left join :IT_LEGO_POLICY_PER_YEAR_SEQ as b
       on a.ZPDEELID  = b.ZPDEELID
      and a.ZVVAANTP  = b.ZVVAANTP
      and a.RAPPORTAGE_MUTATIE_JAAR = b.BEGIN_JAAR				--These two join conditions
      and a.RAPPORTAGE_MUTATIE_JAAR = b.CORRECT_BEGIN_JAAR;		--are the only differences

--For years during which there was no mutation we want to retrieve the latest known information from a previous year
--We mark the years that have a mutation  
  IT_LEGO_PER_YEAR_RANK =
    select *,
           case when zmutdate_for_link is not null then 1 end as flag    
    from :IT_LEGO_PER_YEAR_DIRECT_MATCH;       
      
--Years during which there was no mutation will get the same count as the previous year during which there was a mutation      
  IT_LEGO_PER_YEAR_LINK_KEY =
    select *,
           count(flag) over (partition by ZPDEELID, REPORT_TYPE, ZVVAANTP
                                 order by RAPPORTAGE_MUTATIE_JAAR,flag) as LINK_KEY
    from :IT_LEGO_PER_YEAR_RANK;

--Finally we cascade the key fields forward
  IT_LEGO_PER_YEAR_LINK =
    select a.ZPDEELID,
           a.ZAANSTYP,
           a.ZVZ_GROEP,
           a.ZVVAANTP,
           a.RAPPORTAGE_MUTATIE_JAAR,
           a.REPORT_TYPE,
           b.ZTYPE,
           b.ZEVENTID,
           b.zverw_date_for_link,
           b.MWYZVLGNR_for_link,
           b.zmutdate_for_link
    from :IT_LEGO_PER_YEAR_LINK_KEY a
    left join :IT_LEGO_PER_YEAR_LINK_KEY b
        on a.ZPDEELID    = b.ZPDEELID
       and a.REPORT_TYPE = b.REPORT_TYPE
       and a.ZVVAANTP    = b.ZVVAANTP
       and a.LINK_KEY    = b.LINK_KEY
     where b.flag = 1;  

--Because we created year stands between policy inception and the input parameter from the user, invalid years currently exist
--We want the years during which the mutation was a start (510_ONLY) or a change (BOTH)
--We also want the year during which participant died if the coverage is OP 
--and the year during which the beneficiary died if the coverage PP
--Some additional logic is necessary for the 62000 mutations identified death cases
  IT_LEGO_VALID_PER_YEAR =
    select a.ZPDEELID,
    	   a.ZAANSTYP,
    	   a.ZVZ_GROEP,
    	   a.ZVVAANTP,
    	   a.RAPPORTAGE_MUTATIE_JAAR,
    	   a.REPORT_TYPE,
    	   a.ZTYPE,
    	   a.ZEVENTID,
    	   a.ZVERW_DATE_FOR_LINK,
    	   a.MWYZVLGNR_FOR_LINK,
    	   a.ZMUTDATE_FOR_LINK,
           case 
           --If coverage has not ended before the death date, only show then.
           when b.CORRECTION_APPLIED = 'X' and RAPPORTAGE_MUTATIE_JAAR < left(b.ZSTEDAT,4) and ZAANSTYP = '007' and ZTYPE <> '508_ONLY'
            then 1
           --If coverage has ended before the death date, then do not show in report, because changes have been written to normal mutations already
           when b.CORRECTION_APPLIED = 'X' and RAPPORTAGE_MUTATIE_JAAR < left(b.ZSTEDAT,4) and ZAANSTYP = '007' and ZTYPE = '508_ONLY'
            then 0
           --If coverage has ended the same year as of death date & is not manually filled from previous years.
           when b.CORRECTION_APPLIED = 'X' and RAPPORTAGE_MUTATIE_JAAR = left(b.ZSTEDAT,4) and ZAANSTYP = '007' and RAPPORTAGE_MUTATIE_JAAR = LEFT(zmutdate_for_link,4) and ZTYPE = '508_ONLY'
            then 1                      
-----------CHANGE, WHEN ZAANSTYP NE 007 AND first INSURED DIED THE COVERAGE SHOULD BE ENDED TOO  OR IS THIS NOT POSSIBLE. or is it possible that first insured has cov 51 which ends and cov 11 which does not end. no of course.
			-- when b.CORRECTION_APPLIED = 'X' and RAPPORTAGE_MUTATIE_JAAR = left(b.ZSTEDAT,4) and ZAANSTYP <> '007' and RAPPORTAGE_MUTATIE_JAAR = LEFT(zmutdate_for_link,4) and ZTYPE = '508_ONLY'
             --           then 1
 ---------------------------------------------------------------------------------------           
           --Manually end the policies for years higher than death date. 62000 policy would have not actually ended.
           when b.CORRECTION_APPLIED = 'X' and RAPPORTAGE_MUTATIE_JAAR > left(b.ZSTEDAT,4) and ZAANSTYP = '007'
            then 0
           --when ZTYPE = '508_ONLY' and ZEVENTID = 'E001' and ZAANSTYP = '007' and (RAPPORTAGE_MUTATIE_JAAR-1) = LEFT(zmutdate_for_link,4) and REPORT_TYPE = 'BoY Snapshot'
            --then 1
           --when ZTYPE = '508_ONLY' and ZEVENTID = 'E002' and ZAANSTYP in ('008','015') and (RAPPORTAGE_MUTATIE_JAAR-1) = LEFT(zmutdate_for_link,4) and REPORT_TYPE = 'BoY Snapshot'
            --then 1
           when ZTYPE = '508_ONLY' and ZEVENTID = 'E001' and ZAANSTYP = '007' and RAPPORTAGE_MUTATIE_JAAR = LEFT(zmutdate_for_link,4) and REPORT_TYPE = 'EoY Snapshot'
            then 1
            --------------CHANGE---------------------
          -- when ZTYPE = '508_ONLY' and ZEVENTID = 'E002'             and ZAANSTYP in ('008','015') and RAPPORTAGE_MUTATIE_JAAR = LEFT(zmutdate_for_link,4) and REPORT_TYPE = 'EoY Snapshot'
		     when ZTYPE = '508_ONLY' and ZEVENTID IN ( 'E002', 'B002') and ZAANSTYP in ('008','015') and RAPPORTAGE_MUTATIE_JAAR = LEFT(zmutdate_for_link,4) and REPORT_TYPE = 'EoY Snapshot'
            then 1
           when ZTYPE in ('510_ONLY','BOTH')
            then 1
           end as filter_flag
    from :IT_LEGO_PER_YEAR_LINK a
    left join :IT_LEGO_DEATH b
     on a.ZPDEELID = b.ZPDEELID    where 1=1
    	and a.RAPPORTAGE_MUTATIE_JAAR >= '2000'
     ;


----------------------------------------------------SALARY-----------------------------------------------------
--If there is mutation with an empty salary field, we need to fill it with the latest known salary

--Prepare structure for tablefunction	  
	IT_SALARY_STRUCTURE =
	 select distinct 
	 		ZMUTID, 
	 		ZPDEELID, 
			ZVERW_DAT,
			MWYZVLGNR,
			ZMUTDATE,	 		
	 		ZLTSAL, 
	 		ZLTSALDT, 
	 		ZBSSALAM
	 from :IT_LEGO_RELEVANT_TRANSACTIONSa   -- CHANGE
	;

--call tablefunction to fill in salary gaps
	IT_SALARY_FILLED =
	 select distinct 
	 		ZMUTID, 
	 		ZPDEELID, 
			ZVERW_DAT,
			MWYZVLGNR,
			ZMUTDATE,	 		
	 		ZLTSAL, 
	 		ZLTSALDT, 
	 		ZBSSALAM
	 from "_SYS_BIC"."NNROOT.Z00_Applications.VOD.ReportBase.LEGO::tf_fillsalarygaps"(:IT_SALARY_STRUCTURE)
	;
   
------------------------------------------------------------------------------------------------------------------------------   
-------------------------------------------Postal code ------------------------------------------------------------------------- 
-- Please check this procedure how data is being inserted into below table NNROOT.Z00_Applications.VOD.Reporting.Data::FullLoad_Sterfte_FilmData

IT_DISTINCT_BSN = SELECT DISTINCT BSN FROM :IT_GBB ;

IT_ADDRESS_FINAL = SELECT 
                           a.BSN ,
                           a.SNAPSHOT_TYPE,
						   a.ADDR_VALID_FROM ,
				           a.ADDR_VALID_TO ,
				           a.POSTCODE , 
					       a.BUITENLAND
					FROM   "DM_VOD"."VOD_ADDRESS_INFORMATION" a
					inner join :IT_DISTINCT_BSN b
					on    a.BSN = b.BSN ;
												        												        
	------------------------------------------------------------------------------------------------------------------------------    
	-------------------------------------------INKOMENSKLASSE ------------------------------------------------------------------------- 
	--Goal is to derive For every POSTAL CODE , Fetch INKOMENSKLASSE

IT_INKOMENSKLASSE = SELECT PC as POSTCODE,
	                       HH_INKOM   
					FROM   "_SYS_BIC"."NNROOT.Z00_Applications.VOD.ReportBase.Common/CV_EDM_SET_AGGREGATED" ;
												        												        
------------------------------------------------------------------------------------------------------------------------------ 
-------------------------------------------Snapshot creation + formatting-----------------------------------------------------

--We join the final table back with the base table to fill the columns based on the key.
--Please also refer to the report functional documentation for an overview of the column mapping
  IT_OUTPUT =  
    select 'LEGO' as VTA,
    A.ZEVENTID,
   		 	a.ZPDEELID as POLIS_ID,
			'' as CONTRACT_ID,
			( a.ZPDEELID || '#' || a.ZVVAANTP ) as AANSPRAAK_ID,
			a.RAPPORTAGE_MUTATIE_JAAR  as RAPPORTAGE_MUTATIE_JAAR,
		    to_date(b.ZMUTDATE) as MUTATIE_DATUM,
		    to_timestamp(substring(b.ZMUTID,14,18),'YYYYMMDDHH24MISSFF7') as VERWERKINGS_MOMENT,
		    '0' as COMMERCIEEL_PRODUCT_CODE,
			'' AS COMMERCIEEL_PRODUCT,
		    case when a.ZAANSTYP  in (null, '')
	        	   then '0'
		    	 else a.ZAANSTYP 
		    	end as AANSPRAAK_TYPE,
	        case when b.ZVSTATA in (null, '')
	        	   then '0' 
			     else b.ZVSTATA
		    	end as AANSPRAAK_STATUS,
		    b.ZVFINSTA as AANSPRAAK_FINANCIERINGS_STATUS,
			a.ZVZ_GROEP as VERZEKERINGS_GROEP,
	        case when b.ZVSTATP in (null, '')
	        	   then '0' 
			     else b.ZVSTATP
		    	end as POLIS_STATUS,
	        b.ZINDATPOL as INGANGSDATUM_POLIS,
	        case when (a.ZEVENTID IN ('E001', 'B001') ) --CHANGE
	              and (a.ZAANSTYP = '007')
			       then 0
			     when (a.ZEVENTID IN ( 'E002', 'B002'))    -- CHANGE
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
			       then 0
			     when (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(d.ZSTEDAT,5,4) = '0101'
			       then 0
			     when (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			       then 0
			     else b.ZVERBDR
		    	end as VERZEKERD_BEDRAG,				--has to be zero for death snapshots, user requirement
		    case when (a.ZEVENTID IN ('E001', 'B001') ) --CHANGE
		          and (a.ZAANSTYP = '007')
			       then 0
			     when (a.ZEVENTID IN ( 'E002', 'B002')) --CHANGE
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
			       then 0
			     when (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(d.ZSTEDAT,5,4) = '0101'
			       then 0
			     when (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			       then 0
			     else b.ZTGZ_BDR						--has to be zero for deaths, user requirement
		    	end as TOEGEZEGD_BEDRAG,
			a.ZPDEELID as DEELNEMER_ID,
		
			d.ZGESL as VERZEKERDE_GESLACHT,
			d.ZBSN1 as VERZEKERDE_BSN,
			case when d.ZBSN1 = '' or d.ZBSN1 = '0' or d.ZBSN1 = '000000000' or d.ZBSN1 = '-99999999' or d.ZBSN1 = NULL 
			  then ''
			  else to_nvarchar(HASH_SHA256 (TO_BINARY(d.ZBSN1))) 
			end as VERZEKERDE_BSN_HASHED,
			to_date(d.ZGEBDAT,'YYYYMMDD') as VERZEKERDE_GEB_DATUM,
			to_date(d.ZSTEDAT,'YYYYMMDD') as VERZEKERDE_STERFTE_DATUM,
			case when d.ZGEBDAT = ''						 
			      then null
			     when a.ZEVENTID IN  ('E001', 'B001')  -- CHANGE	 			
			      then null
			     when year(b.ZMUTDATE) >= year(d.ZSTEDAT)	--NCZ-1903: the BoY snapshot of the year in which the 
			      and (a.RAPPORTAGE_MUTATIE_JAAR > year(d.ZSTEDAT))
			      and (a.REPORT_TYPE = 'BoY Snapshot')		--participant died should show the age, because the participant
			      then null									--is still alive then
			     when year(b.ZMUTDATE) >= year(d.ZSTEDAT)	--For the EoY snapshot this is not the case 
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			      then null 
			     when (a.RAPPORTAGE_MUTATIE_JAAR >= year(d.ZSTEDAT))		
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(d.ZSTEDAT,5,4) = '0101'
			      then null
			     when (a.RAPPORTAGE_MUTATIE_JAAR >= year(d.ZSTEDAT))
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			      then null
			     else (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZGEBDAT)) 
		    	end as VERZEKERDE_LEEFTIJD,				--If participant is dead then we want it to be zero, user requirement
	        case when d.ZGEBDAT = ''
			      then null
			     when (a.ZEVENTID IN ('E001', 'B001')  )   --CHANGE
			      and (year(b.ZMUTDATE) = a.RAPPORTAGE_MUTATIE_JAAR)
			      then (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZGEBDAT))
			     when  (year(d.ZSTEDAT) = a.RAPPORTAGE_MUTATIE_JAAR)
			       and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(d.ZSTEDAT,5,4) = '0101'
			      then (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZGEBDAT))
			     when  (year(d.ZSTEDAT) = a.RAPPORTAGE_MUTATIE_JAAR)
			       and (a.REPORT_TYPE = 'EoY Snapshot')
			      then (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZGEBDAT))
			     else null 
		    	end as VERZEKERDE_STERFTE_LEEFTIJD,		--filled in only for year of death
		    '' as VERZEKERDE_ROOK_STATUS,
		verzekerde_address.POSTCODE as VERZEKERDE_POST_CODE,
		verzekerde_address.BUITENLAND  as VERZEKERDE_LAND, 
			d.ZVZGESL as VERZORGDE_GESLACHT,
		    d.ZVZBSN as VERZORGDE_BSN,
		    case when d.ZVZBSN = '' or d.ZVZBSN = '0' or d.ZVZBSN = '000000000' or d.ZVZBSN = '-99999999' or d.ZVZBSN = NULL
			 then ''
			 else to_nvarchar(HASH_SHA256 (TO_BINARY(d.ZVZBSN))) 
			end as VERZORGDE_BSN_HASHED, 
            to_date(d.ZVZGEBDAT) as VERZORGDE_GEB_DATUM,
		    to_date(d.ZVZSTEDAT) as VERZORGDE_STERFTE_DATUM,
	        case when (a.ZEVENTID IN ('E002', 'B002'))   	-- CHANGE	
			       then null
			     when a.ZAANSTYP not in ('008','015')
			       then null
			     when (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT)) > '999' --exception handling, some weird birth dates exist
			       then null
			     when (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT)) < '0'	--exception handling, some weird birth dates exist
			       then null
			     else (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT)) 
		       end as VERZORGDE_LEEFTIJD,				--only filled in for partner pension and if still alive. 
	       case when (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT)) > '999'
	              then null
			    when (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT)) < '0'
			      then null
	            when (a.ZEVENTID IN ('E002', 'B002')) 		-- CHANGE	
	             and (a.ZAANSTYP in ('008','015'))
			      then (a.RAPPORTAGE_MUTATIE_JAAR-year(d.ZVZGEBDAT))
			    else null
		   	   end as VERZORGDE_STERFTE_LEEFTIJD,	--only filled in for partner pension in year of death
		   '' as VERZORGDE_ROOK_STATUS,
		verzorgde_address.POSTCODE as VERZORGDE_POST_CODE,
		     verzorgde_address.BUITENLAND  as VERZORGDE_LAND, 
	       case when d.CORRECTION_APPLIED = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	       		 and (a.ZAANSTYP = '007')
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZSTEDAT),5,4) = '0101'
	          	  then c.ZVSTATPV
	          	when d.CORRECTION_APPLIED = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	       		 and (a.ZAANSTYP = '007')
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c.ZVSTATPV
	          	when (a.ZEVENTID IN ('E001')  and a.ZAANSTYP = '007')  
	          		or (a.ZEVENTID = 'E002' and a.ZAANSTYP in ('008','015'))	  
	         		then b.ZVSTATPV 
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVSTATPV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVSTATPV
	-----------END CHANGE-----------------------------------------------
	         	else null		
	        end as VORIGE_POLIS_STATUS,			--all VORIGE_xxx fields are filled in case of death, users want to see the situation before death
	        case when d.CORRECTION_APPLIED = 'X' 
	              and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	              and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(to_date(d.ZSTEDAT),5,4) = '0101'
	          	   then c.ZVVSTATPV
	          	 when d.CORRECTION_APPLIED = 'X' 
	              and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	              and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
	          	   then c.ZVVSTATPV
	          	 when (a.ZEVENTID IN ('E001')  and a.ZAANSTYP = '007') 
	          		or (a.ZEVENTID = 'E002' and a.ZAANSTYP in ('008','015'))	   
	         	 	then b.ZVVSTATPV 
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVVSTATPV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVVSTATPV
	-----------END CHANGE-----------------------------------------------
	         	 else null		 
	        end as VORIGE_VTA_POLIS_STATUS,
	        case when d.CORRECTION_APPLIED = 'X' 
	        	  and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	        	  and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(to_date(d.ZSTEDAT),5,4) = '0101'
	          	   then coalesce(c.ZVSTATAV, 0)
	         	 when d.CORRECTION_APPLIED = 'X' 
	        	  and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	        	  and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
	          	   then coalesce(c.ZVSTATAV, 0)
	          	 when a.ZEVENTID IN ('E001')  and a.ZAANSTYP = '007'   
	         	 	then b.ZVSTATAV 
	         	 when a.ZEVENTID = 'E002' and a.ZAANSTYP in ('008','015') 
	         	 	then b.ZVSTATAV	  
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVSTATAV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVSTATAV
	-----------END CHANGE-----------------------------------------------	         	 	        	   
	         	 else null	
			end as VORIGE_AANSPRAAK_STATUS,
	        case --1st insured death(62000 policy) + Death date on 1st jan for BoY Report. 
		    	 when d.CORRECTION_APPLIED = 'X' 
	              and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	              and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(to_date(d.ZSTEDAT),5,4) = '0101'
	          	   then c.ZVVSTATAV
	          	 --1st insured death(62000 policy) + EoY Report. 
	         	 when d.CORRECTION_APPLIED = 'X' 
	              and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
	              and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
	          	   then c.ZVVSTATAV
	          	 when a.ZEVENTID IN ('E001')  and a.ZAANSTYP = '007'   
	         	 	then b.ZVVSTATAV 
	         	 when a.ZEVENTID = 'E002' and a.ZAANSTYP in ('008','015') 
	         	 	then b.ZVVSTATAV
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVVSTATAV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVVSTATAV
	-----------END CHANGE----------------------------------------------- 	         	 	
	         	 else null	
	         end as VORIGE_VTA_AANSPRAAK_STATUS,  	
		    case --1st insured death  
			     when (a.ZEVENTID IN ('E001') )   
			      and (a.ZAANSTYP = '007')
			      and (a.ZTYPE = '508_ONLY')
			     then b.ZVERBDRO
			     --62000 policies + Coverage 7 death + Death date on 1st jan for BoY Report. 
			     when d.CORRECTION_APPLIED = 'X' 
			      and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(to_date(d.ZSTEDAT),5,4) = '0101'
			       then c.ZVERBDRO
			     --62000 policies + Coverage 7 death + EoY Report 
			     when d.CORRECTION_APPLIED = 'X' 
			      and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			       then c.ZVERBDRO
			     else 0
		    end as VORIGE_VERZEKERD_BEDRAG,
	        case --1st insured death  
			     when (a.ZEVENTID IN ('E001') )   
			      and (a.ZAANSTYP = '007')
			      and (a.ZTYPE = '508_ONLY')
			     then b.ZTGZ_BDRO
			     --62000 policies + Coverage 7 death + Death date on 1st jan for BoY Report.
			     when d.CORRECTION_APPLIED = 'X' 
			      and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'BoY Snapshot')
			      and substring(to_date(d.ZSTEDAT),5,4) = '0101'
			       then c.ZTGZ_BDO
			     --62000 policies + Coverage 7 death + EoY Report 
			     when d.CORRECTION_APPLIED = 'X' 
			      and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZSTEDAT)) 
			      and (a.ZAANSTYP = '007')
			      and (a.REPORT_TYPE = 'EoY Snapshot')
			       then c.ZTGZ_BDO
			       
			       
			     else 0
		    end as VORIGE_TOEGEZEGD_BEDRAG,

	        case --2nd Insured death + Ending for coverage 8,15
	        	 when (a.ZEVENTID = 'E002') 
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
	        	  and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	          	   then coalesce(b.ZVSTATAV, 0)
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVSTATAV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVSTATAV
	-----------END CHANGE-----------------------------------------------		          	   
	         	 else 0
			end as VORIGE_VERZORGDE_AANSPRAAK_STATUS, 
	        case --2nd Insured death + Ending for coverage 8,15
	        	 when (a.ZEVENTID = 'E002') 
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
	        	  and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT))
	          	   then coalesce(b.ZVVSTATAV, '') 
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVVSTATAV
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVVSTATAV
	-----------END CHANGE----------------------------------------------- 	       	          	   
	         	 else ''
	         end as VORIGE_VERZORGDE_VTA_AANSPRAAK_STATUS,

		    case --2nd Insured death + Ending for coverage 8,15
			     when (a.ZEVENTID = 'E002') 
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
			       then b.ZVERBDRO
	--------------ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZVERBDRO
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZVERBDRO
	-----------END CHANGE-----------------------------------------------				       
		    	 else 0
		    end as VORIGE_VERZORGDE_VERZEKERD_BEDRAG,
		    case --2nd Insured death + Ending for coverage 8,15
			     when (a.ZEVENTID = 'E002') 
			      and (a.ZAANSTYP in ('008','015'))
			      and (a.ZTYPE = '508_ONLY')
			       then b.ZTGZ_BDRO
	--------------CHANGE ADD CODE FOR B002-------------------------         		
		         when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'BoY Snapshot')
			     and substring(to_date(d.ZVZSTEDAT),5,4) = '0101'
	          	  then c1.ZTGZ_BDO
	          	when d.CORRECTION_APPLIED2 = 'X' 
	       		 and (a.RAPPORTAGE_MUTATIE_JAAR = year(d.ZVZSTEDAT)) 
	       		 and (a.ZAANSTYP IN ('008', '015'))
			     and (a.REPORT_TYPE = 'EoY Snapshot')
	          	  then c1.ZTGZ_BDO
	-----------END CHANGE-----------------------------------------------				       
		    	 else 0
		    end as VORIGE_VERZORGDE_TOEGEZEGD_BEDRAG,

	        b.ZWERKGID as WERKGEVER_CONTRACT_ID,
  		    salary.ZLTSAL as SALARIS,
	        salary.ZLTSALDT as DATUM_VASTSTELLING_SALARIS,
  		    salary.ZBSSALAM as SALARIS_PENSIOENGRONDSLAG,
			b.MBETTCP as BETALING_TERM,
		    b.MBETMCP as BETALING_MOMENT,
			b.ZVTAID as VTA_ID,
		    a.ZVVAANTP as VTA_AANSPRAAK_TYPE,
	        b.ZVVSTATA as VTA_AANSPRAAK_STATUS,	
		    b.ZVVSTATP as VTA_POLIS_STATUS,				
			null as PENSIOEN_LEEFTIJD,
		    d.CORRECTION_APPLIED AS CORRECTION_APPLIED,
		    case when a.ZTYPE = '508_ONLY'					--When we selected the valid years, we ensured that years in which the
		    		then 'Death Snapshot'					--latest mutation was closure are only included in case of death
		    	 else a.REPORT_TYPE							--So these are marked death snapshot, all other years as EOY/BOY snapshot
		    	 end as SNAPSHOT,			    
		 verzekerde_hh_inkom.HH_INKOM as  VERZEKERDE_INKOMENSKLASSE ,
		 verzorgde_hh_inkom.HH_INKOM  as VERZORGDE_INKOMENSKLASSE,
		 0 as "VERZEKERDE_RATIO_TOEGEZEGD_BEDRAG",
		 0 as "VERZORGDE_RATIO_TOEGEZEGD_BEDRAG"
    from :IT_LEGO_VALID_PER_YEAR as a					--Year snapshots + latest mutation key fields + latest salary + personal information
    left join :IT_LEGO_RELEVANT_TRANSACTIONSa as b			--To retrieve the relevant fields from the latest mutation,     -- CHANGE FROM :IT_LEGO_RELEVANT_TRANSACTIONS
 	    on a.ZPDEELID  			 = b.ZPDEELID				--or VORIGE_xxx fields which we added as seperate fields to each mutation (only in case of death)
       and a.ZVVAANTP  			 = b.ZVVAANTP
       and a.zverw_date_for_link = b.ZVERW_DAT
       and a.MWYZVLGNR_for_link  = b.MWYZVLGNR
       and a.zmutdate_for_link   = b.ZMUTDATE
    left join :IT_62000_PREV_INFO  c						--for the additional deaths identified due to 62000 mutations
        on a.ZPDEELID 			 = c.ZPDEELID
       left join :IT_62000_PREV_INFO2  c1						--CHANGE  --  for the additional deaths identified due to B002 mutation
        on a.ZPDEELID 			 = c1.ZPDEELID     
        left join :IT_GBB_MUTATION as d							--retrieve personal info
        on a.ZPDEELID = d.ZPDEELID             
    left outer join :IT_SALARY_FILLED as salary
       on  a.ZPDEELID            = salary.ZPDEELID
       and a.zverw_date_for_link = salary.ZVERW_DAT
       and a.MWYZVLGNR_for_link  = salary.MWYZVLGNR
       and a.zmutdate_for_link   = salary.ZMUTDATE 
    left join :IT_ADDRESS_FINAL as verzekerde_address		--retrieve postalcode info first insuranced
       on d.ZBSN1 = verzekerde_address.BSN 
       and a.RAPPORTAGE_MUTATIE_JAAR >= verzekerde_address.ADDR_VALID_FROM
       AND a.RAPPORTAGE_MUTATIE_JAAR < verzekerde_address.ADDR_VALID_TO
       AND a.REPORT_TYPE = verzekerde_address.SNAPSHOT_TYPE
    left join :IT_ADDRESS_FINAL as verzorgde_address				--retrieve postalcode info second insuranced
       on d.ZVZBSN = verzorgde_address.BSN 
       and a.RAPPORTAGE_MUTATIE_JAAR >= verzorgde_address.ADDR_VALID_FROM   
       and a.RAPPORTAGE_MUTATIE_JAAR < verzorgde_address.ADDR_VALID_TO  
       AND a.REPORT_TYPE = verzorgde_address.SNAPSHOT_TYPE
    left join :IT_INKOMENSKLASSE  as verzekerde_hh_inkom
       on verzekerde_hh_inkom.POSTCODE = verzekerde_address.POSTCODE
    left join :IT_INKOMENSKLASSE   as verzorgde_hh_inkom
       on verzorgde_hh_inkom.POSTCODE = verzorgde_address.POSTCODE 
     where  
     	a.filter_flag = 1            
     ;

END;