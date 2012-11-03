
INSTALL ( PROGRAMS checkAndMove
 		   condor_clusters
		   mu2eart
		   mu2emars
          DESTINATION ${product}/${version}/bin )

INSTALL ( FILES mu2egrid.pm
          DESTINATION ${product}/${version}/bin )

add_subdirectory(impl)
