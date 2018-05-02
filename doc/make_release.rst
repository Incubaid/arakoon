How to make a release
=====================

 * Run the tools/make_release_branch.py script which

   - creates the X.Y.Z branch
   - updates debian changelog
   - commits branch => ${REV}

 * Jenkins: arakoon-release-qp4

 * Jenkins: arakoon-release-deb

 * Bitbucket: first download artefacts (2 .debs, 1 .egg) and 
   then upload these to bitbucket downloads

 * Jira: resolve issues and release X.Y.Z branch

 * Git: gh-pages branch (src)
 
   ** python make_download.py --version X.Y.Z --rev ${REV}
 
   ** python make_release.py --version X.Y.Z --rev ${REV} > releases/X.Y.Z.rst

   ** commit git

   ** python build.py
 
   ** check local site

   ** commit 
  
   ** push

 * wait until github published it.

 * send mail to arakoon@openvstorage.com

