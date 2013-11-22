# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
	config.vm.box = "precise64"

	config.vm.provider :virtualbox do |vb|
	  vb.customize ["modifyvm", :id, "--memory", "1024"]
	end

	config.vm.provision "shell", path: "install_packages.sh"
end
