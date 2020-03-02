import json
import requests
import sys
sys.path.append('..')
from client import Client
from datetime import datetime
import time

class GithubData(Client):
    def __init__(self,coin_repo, coin_name):
        self.start_client()
        self.username = 'Toeestudio'
        self.access_token = '57a90c699f5e0f019b6dd0f79fbe0a13ff9764d7'
        self.database_name = 'heimdall'
        self.coin_name = 'coin_name'
        self.crawler_name = 'coin_name'+'_git_data'
        self.url = 'coin_repo'

    def start_git_session(self,url):
        gh_session = requests.Session()
        gh_session.auth = (self.username, self.access_token)
        r = gh_session.get(url)
        return r.headers, r.json()

    def get_number_of_pages(self,url):
        r = self.start_git_session(url)[0]
        if 'Link' in r:
            header_array = r['Link'].split(',')
            for link in header_array:
                if 'rel="last"' in link:
                    return link[link.find("=")+1:link.find(">")]
        else:
            return 1

    def get_repos_urls(self, repos_page, url):
        repos_url = []
        for i in range(int(repos_page)):
            response = self.start_git_session(url+'?page='+str((i+1)))[1]
            for j in range(len(response)):
                repos_url.append(response[j]['url'])
        return repos_url

    def get_closed_issues_url(self, repos_url):
        closed_issues = []
        for repo in repos_url:
            closed_issues_url = self.start_git_session(repo)[1]['issues_url']
            parsed_url = closed_issues_url[:closed_issues_url.find("{")]
            closed_issues.append(parsed_url)
        return closed_issues

    def get_pull_requests(self, repos_url):
        pull_requests = []
        for repo in repos_url:
            pulls_url = self.start_git_session(repo)[1]['pulls_url']
            parsed_url = pulls_url[:pulls_url.find("{")]
            pull_requests.append(parsed_url)
        return pull_requests

    def get_commits_url(self, repos_url):
        commits_url = []
        for repo in repos_url:
            commit_url = self.start_git_session(repo)[1]['commits_url']
            parsed_url = commit_url[:commit_url.find("{")]
            commits_url.append(parsed_url)
        return commits_url

    def get_contributors_url(self,repos_url):
        contributors_url = []
        for repo in repos_url:
            contributor_url = self.start_git_session(repo)[1]['contributors_url']
            contributors_url.append(contributor_url)
        return contributors_url

    def get_closed_issues_pages_url(self,closed_issues_url,closed_issues_pages):
        closed_issues_array = []
        pages_list = list(closed_issues_pages)
        zipped_list = zip(closed_issues_url,pages_list)
        for u, p in zipped_list:
            for i in range(int(p)):
                closed_issues_array.append(u+'?state=closed&page='+str(i+1))
        return closed_issues_array

    def get_commits_and_contributors_pages_url(self,commits_url,commits_pages):
        commits_array = []
        pages_list = list(commits_pages)
        zipped_list = zip(commits_url,pages_list)
        for u, p in zipped_list:
            for i in range(int(p)):
                commits_array.append(u+'?page='+str(i+1))
        return commits_array
         
    def get_commits_and_contributors_number(self,urls):
        total_commits = 0
        for url in urls:
            response = self.start_git_session(url)[1]
            total_commits += len(response)
        return total_commits

    def get_stars_forks_wacthers_issues(self,urls):
        repo_stars = 0
        repo_forks = 0
        repo_watchers = 0
        repo_issues = 0
        for url in urls:
            repo_stars += self.start_git_session(url)[1]['stargazers_count']
            repo_forks += self.start_git_session(url)[1]['forks_count']
            repo_watchers += self.start_git_session(url)[1]['watchers_count']
            repo_issues += self.start_git_session(url)[1]['open_issues_count']
        return repo_stars, repo_forks, repo_watchers, repo_issues

    def format_data(self,commits_number,contributors_number,open_issues_number,pull_requests,stars,forks,watchers,closed_issues_number):
        json={
            'database':self.database_name,
            'table':self.crawler_name,
            'data':{
                'datetime': str(datetime.utcnow()),
                'commits_number': commits_number,
                'contributors_number': contributors_number,
                'open_issues_number': open_issues_number,
                'pull_requests' : pull_requests,
                'stars_number': stars,
                'forks' : forks,
                'watchers': watchers,
                'closed_issues_number':closed_issues_number
            }
        }
        return json

    def run(self):
        while True:
            try:
                repo_info = self.start_git_session(self.url)[1]                
                repos_link = repo_info.get("repos_url","")
                repos_page = self.get_number_of_pages(repos_link)          
                repos_url = self.get_repos_urls(repos_page,repos_link)

                commits_url = self.get_commits_url(repos_url)
                contributors_url=self.get_contributors_url(repos_url)
                pulls_url = self.get_pull_requests(repos_url)
                closed_issues_url = self.get_closed_issues_url(repos_url)

                commits_pages = map(self.get_number_of_pages, commits_url)
                contributors_pages = map(self.get_number_of_pages, contributors_url)
                pulls_pages = map(self.get_number_of_pages, pulls_url)
                closed_issues_pages = map(self.get_number_of_pages, closed_issues_url)

                contributors_pages_url = self.get_commits_and_contributors_pages_url(contributors_url,contributors_pages)
                commits_pages_url=self.get_commits_and_contributors_pages_url(commits_url,commits_pages)
                pulls_pages_url = self.get_commits_and_contributors_pages_url(pulls_url, pulls_pages)
                closed_issues_pages_url = self.get_closed_issues_pages_url(closed_issues_url,closed_issues_pages)

                commits_number = self.get_commits_and_contributors_number(commits_pages_url)
                contributors_number = self.get_commits_and_contributors_number(contributors_pages_url)
                pull_requests_number = self.get_commits_and_contributors_number(pulls_pages_url)
                closed_issues_number = self.get_commits_and_contributors_number(closed_issues_pages_url)
                
                stars = self.get_stars_forks_wacthers_issues(repos_url)[0]      
                forks = self.get_stars_forks_wacthers_issues(repos_url)[1]     
                watchers = self.get_stars_forks_wacthers_issues(repos_url)[2]      
                issues = self.get_stars_forks_wacthers_issues(repos_url)[3]      
                formatted_data = self.format_data(commits_number, contributors_number, issues, pull_requests_number, stars, forks, watchers, closed_issues_number)            
                self.send(formatted_data)
                
                #print(repos_link)
            except Exception as e:
                self.exception(e, self.crawler_name)
                time.sleep(30)

if __name__ == "__main__":
    try:
        if len(sys.argv) == 3:
            coin_repo = str(sys.argv[1])
            coin_name = str(sys.argv[2])
    except ValueError as e:
        print("o valor de entrada não esta em conformidade com o esperado")
        raise e
    gt_class = GithubData(coin_repo,coin_name)
    
    from table_creator import *

    with Creator() as creator:
        creator.try_create_database(gt_class.crawler_name,commits_number=float, contributors_number=float, open_issues_number=float, pull_requests_number=float, stars_number=float, forks=float,watchers=float,closed_issues_number=float)
    
    gt_class.run()
    
