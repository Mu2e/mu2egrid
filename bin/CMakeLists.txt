
INSTALL ( PROGRAMS checkAndMove
 		   condor_clusters
		   mu2eart
          DESTINATION ${product}/${version}/bin )

add_subdirectory(impl)
