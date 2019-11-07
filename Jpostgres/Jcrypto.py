import requests
import json
import sys

if sys.version_info[0] > 2:
    import configparser
else:
    import ConfigParser as configparser


class JKMService(object):
    config = configparser.RawConfigParser()
    config.read('./pgcopy.cfg')

    def __init__(self):
        pass

    def decrypt(self, project_id, system_seq, credential_id):
        decrypt_headers = {"Content-Type": "application/json"}
        self.project_id = str(project_id)
        self.system_seq = str(system_seq)
        self.credential_id = str(credential_id)

        kms_decryption_url = self.config.get('KMS', 'kms_decryption_url')

        print(kms_decryption_url)
        data = json.dumps({"projectId": self.project_id, "systemSeq": self.system_seq, "credentialId": self.credential_id})
        print(data)
        try:
            response = requests.post(url=kms_decryption_url, headers=decrypt_headers, data=data)
            print("{}".format(response.status_code))
            print(response.text)
            if (response.status_code == 200):
                kms_pwd = json.loads(response.text)['message']
                return kms_pwd
            else:
                raise Exception

        except Exception as e:
            print('error {}'.format(response.text))


if __name__ == '__main__':

    headers = {"Content-Type": "application/json"}
    projectId = "100"
    systemSeq = "46"
    credential_id = "31"
    kms_obj = JKMService()c
    print(kms_obj.decrypt(projectId, systemSeq, credential_id))
