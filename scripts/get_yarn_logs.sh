# script for printing yarn logs of certain application (task) into stdout
# the only mandatory argument to the script is applicationID, 
# e.g. "application_1615997632012_0001".
# Useful for debugging failed applications.

sudo -u spark yarn logs -applicationId $1
