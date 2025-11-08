SELECT  customerId, upper(firstname) as firstname , upper(lastname) as lastname, 
concat(firstname, lastname) as fullname, 
email, city FROM [dbo].[Customer]