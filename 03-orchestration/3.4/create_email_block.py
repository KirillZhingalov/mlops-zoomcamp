from prefect_email import EmailServerCredentials

def create_email_block():
    email_server_creds = EmailServerCredentials()
    email_server_creds.save(name="my-email-server-creds", overwrite=True)


if __name__ == "__main__":
    create_email_block()
    