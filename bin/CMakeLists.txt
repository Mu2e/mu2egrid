
INSTALL ( PROGRAMS checkAndMove
 		   mu2e_clusters
		   mu2eart
		   mu2eg4bl
		   mu2emars
		   mu2eprodsys
          DESTINATION ${product}/${version}/bin )

INSTALL ( FILES mu2egrid.pm
          DESTINATION ${product}/${version}/bin )

add_subdirectory(impl)
