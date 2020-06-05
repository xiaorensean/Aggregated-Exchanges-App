import requests
import json

# Credential Info
auth_url = 'https://api.refinitiv.com:443/auth/oauth2/v1/token'
scope = 'trapi'
user = 'GE-A-01669631-3-3373'
password = 'k-!\"*dSg6(%[<vntbMLDd\\*aF]=p\"AWcAyQ^Bf>\\'
clientid = '2bd36ad5581c4ff0b9914641a40480316ab28105'
client_secret = ''



def get_sts_token(current_refresh_token, url=None):
    """
        Retrieves an authentication token.
        :param current_refresh_token: Refresh token retrieved from a previous authentication, used to retrieve a
        subsequent access token. If not provided (i.e. on the initial authentication), the password is used.
    """

    if url is None:
        url = auth_url

    if not current_refresh_token:  # First time through, send password
        if url.startswith('https'):
            data = {'username': user, 'password': password, 'grant_type': 'password', 'takeExclusiveSignOnControl': True,
                    'scope': scope}
        else:
            data = {'username': user, 'password': password, 'client_id': clientid, 'grant_type': 'password', 'takeExclusiveSignOnControl': True,
                    'scope': scope}
        print("Sending authentication request with password to", url, "...")
    else:  # Use the given refresh token
        if url.startswith('https'):
            data = {'username': user, 'refresh_token': current_refresh_token, 'grant_type': 'refresh_token'}
        else:
            data = {'username': user, 'client_id': clientid, 'refresh_token': current_refresh_token, 'grant_type': 'refresh_token'}
        print("Sending authentication request with refresh token to", url, "...")

    try:
        if url.startswith('https'):
            # Request with auth for https protocol
            r = requests.post(url,
                              headers={'Accept': 'application/json'},
                              data=data,
                              auth=(clientid, client_secret),
                              verify=True,
                              allow_redirects=False)
        else:
            # Request without auth for non https protocol (e.g. http)
            r = requests.post(url,
                              headers={'Accept': 'application/json'},
                              data=data,
                              verify=True,
                              allow_redirects=False)

    except requests.exceptions.RequestException as e:
        print('EDP-GW authentication exception failure:', e)
        return None, None, None

    if r.status_code == 200:
        auth_json = r.json()
        print("EDP-GW Authentication succeeded. RECEIVED:")
        print(json.dumps(auth_json, sort_keys=True, indent=2, separators=(',', ':')))

        return auth_json['access_token'], auth_json['refresh_token'], auth_json['expires_in']
    elif r.status_code == 301 or r.status_code == 302 or r.status_code == 307 or r.status_code == 308:
        # Perform URL redirect
        print('EDP-GW authentication HTTP code:', r.status_code, r.reason)
        new_host = r.headers['Location']
        if new_host is not None:
            print('Perform URL redirect to ', new_host)
            return get_sts_token(current_refresh_token, new_host)
        return None, None, None
    elif r.status_code == 400 or r.status_code == 401:
        # Retry with username and password
        print('EDP-GW authentication HTTP code:', r.status_code, r.reason)
        if current_refresh_token:
            # Refresh token may have expired. Try using our password.
            print('Retry with username and password')
            return get_sts_token(None)
        return None, None, None
    elif r.status_code == 403 or r.status_code == 451:
        # Stop retrying with the request
        print('EDP-GW authentication HTTP code:', r.status_code, r.reason)
        print('Stop retrying with the request')
        return None, None, None
    else:
        # Retry the request to the API gateway
        print('EDP-GW authentication HTTP code:', r.status_code, r.reason)
        print('Retry the request to the API gateway')
        return get_sts_token(current_refresh_token)

if __name__ == "__main__":
    a = get_sts_token(None)
    print(a)
