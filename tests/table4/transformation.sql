SELECT  customerId, name as firstname , upper(lastname) as lastname,
concat(firstname, lastname) as fullname, 
email, city FROM [dbo].[Customer]