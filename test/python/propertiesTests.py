import unittest
from propertiesTransformer import *

class MetricsTestCase(unittest.TestCase):
    def test(self):
        data = '{"Id":"efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f","Created":"2016-04-05T12:00:24.189927146Z","Path":"/entrypoint.sh","Args":["mysqld"],"Config":{"Hostname":"default","ExposedPorts":{"3306/tcp":{}},"Env":["MYSQL_ROOT_PASSWORD=test","PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin","MYSQL_MAJOR=5.7","MYSQL_VERSION=5.7.9-1debian8"],"Cmd":["mysqld"],"Image":"mysql","Volumes":{"/var/lib/mysql":{}},"Entrypoint":["/entrypoint.sh"]},"State":{"Running":true,"Pid":9864,"StartedAt":"2016-04-10T12:00:02.857433067Z","FinishedAt":"2016-04-06T13:57:45.221317644Z"},"Image":"c29b1276a98ab5da05ca3cd1a97690767cb7f15478e1fd2d2b35fcd7e47b6f4e","NetworkSettings":{"SandboxKey":"/var/run/docker/netns/default"},"ResolvConfPath":"/mnt/sda1/var/lib/docker/containers/efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f/resolv.conf","HostnamePath":"/mnt/sda1/var/lib/docker/containers/efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f/hostname","HostsPath":"/mnt/sda1/var/lib/docker/containers/efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f/hosts","LogPath":"/mnt/sda1/var/lib/docker/containers/efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f/efeff5f0989c854357222ad1df2a6b1f94ec962d92766d115c677d5ff415f01f-json.log","Name":"/db","Driver":"aufs","Mounts":[{"Source":"/mnt/sda1/var/lib/docker/volumes/07f155fe982cdd2a2113f135f270f13f8a06a0757f2579011eec52a77cac0f65/_data","Destination":"/var/lib/mysql","Mode":"","RW":true}],"HostConfig":{"NetworkMode":"host","RestartPolicy":{"Name":"no"},"LogConfig":{"Type":"json-file"},"MemorySwappiness":-1}}'
        result = createContainerDict(data, "foo_0", "foo", "container", "default")
        print result
        self.assertTrue(result["name"] == "db")
        self.assertTrue(result["memory_swappiness"] == -1)
        self.assertTrue(result["image"] == 'c29b1276a98ab5da05ca3cd1a97690767cb7f15478e1fd2d2b35fcd7e47b6f4e')
        self.assertTrue("MYSQL_ROOT_PASSWORD=test" in result["environment"] \
                        and "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" in result["environment"] \
                        and "MYSQL_MAJOR=5.7" in result["environment"] \
                        and "MYSQL_VERSION=5.7.9-1debian8" in result["environment"])
        self.assertTrue(result["host_id"] == "default")
        self.assertTrue(result["restart_policy"] == "no")
        self.assertTrue(result["log_driver"] == "aufs")

if __name__ == '__main__':
    unittest.main()