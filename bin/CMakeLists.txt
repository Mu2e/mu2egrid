
INSTALL ( PROGRAMS checkAndMove
 		   condor_clusters
		   mu2esub
          DESTINATION ${product}/${version}/bin )

add_subdirectory(impl)
