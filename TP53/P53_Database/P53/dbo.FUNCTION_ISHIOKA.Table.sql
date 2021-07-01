USE [MMB_P53]
GO
/****** Object:  Table [dbo].[FUNCTION_ISHIOKA]    Script Date: 26/10/2020 11:45:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FUNCTION_ISHIOKA](
	[Function_Ishioka_ID] [smallint] NOT NULL,
	[AAchange_old] [nvarchar](10) NOT NULL,
	[AAchangeID] [smallint] NULL,
	[Codon72AA] [char](2) NULL,
	[WAF1_class] [smallint] NULL,
	[MDM2_class] [smallint] NULL,
	[BAX_class] [smallint] NULL,
	[h1433s_class] [smallint] NULL,
	[AIP1_class] [smallint] NULL,
	[GADD45_class] [smallint] NULL,
	[NOXA_class] [smallint] NULL,
	[P53R2_class] [smallint] NULL,
	[Consistent_class] [smallint] NULL,
	[WAF1nWT] [decimal](18, 1) NULL,
	[MDM2nWT] [decimal](18, 1) NULL,
	[BAXnWT] [decimal](18, 1) NULL,
	[h1433snWT] [decimal](18, 1) NULL,
	[AIP1nWT] [decimal](18, 1) NULL,
	[GADD45nWT] [decimal](18, 1) NULL,
	[NOXAnWT] [decimal](18, 1) NULL,
	[P53R2nWT] [decimal](18, 1) NULL,
	[WAF1_TA] [smallint] NULL,
	[MDM2_P2_TA] [smallint] NULL,
	[BAX_TA] [smallint] NULL,
	[h1433s_TA] [smallint] NULL,
	[AIP1_TA] [smallint] NULL,
	[GADD45_TA] [smallint] NULL,
	[NOXA_TA] [smallint] NULL,
	[P53R2_TA] [smallint] NULL,
	[WAF1nWT_Saos2] [decimal](18, 1) NULL,
	[MDM2nWT_Saos2] [decimal](18, 1) NULL,
	[BAXnWT_Saos2] [decimal](18, 1) NULL,
	[h1433snWT_Saos2] [decimal](18, 1) NULL,
	[AIP1nWT_Saos2] [decimal](18, 1) NULL,
	[PUMAnWT_Saos2] [decimal](18, 1) NULL,
	[SubG1nWT_Saos2] [decimal](18, 1) NULL,
	[Oligomerisation_yeast] [char](15) NULL,
	[TAclass2] [nvarchar](50) NULL,
 CONSTRAINT [PK_FUNCTION_ISHIOKA_Function_Ishioka_ID] PRIMARY KEY CLUSTERED 
(
	[Function_Ishioka_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[FUNCTION_ISHIOKA]  WITH NOCHECK ADD  CONSTRAINT [FK_FUNCTION_ISHIOKA_AAchangeID] FOREIGN KEY([AAchangeID])
REFERENCES [dbo].[AA_change] ([AAchange_ID])
ON UPDATE CASCADE
GO
ALTER TABLE [dbo].[FUNCTION_ISHIOKA] CHECK CONSTRAINT [FK_FUNCTION_ISHIOKA_AAchangeID]
GO
ALTER TABLE [dbo].[FUNCTION_ISHIOKA]  WITH NOCHECK ADD  CONSTRAINT [CK_FUNCTION_ISHIOKA_Codon72AA] CHECK  (([Codon72AA] = 'R' or [Codon72AA] = 'P' or [Codon72AA] = 'NA'))
GO
ALTER TABLE [dbo].[FUNCTION_ISHIOKA] CHECK CONSTRAINT [CK_FUNCTION_ISHIOKA_Codon72AA]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'WAF1nWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'MDM2nWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'BAXnWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'h1433snWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'AIP1nWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'GADD45nWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'NOXAnWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'P53R2nWT'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'WAF1_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'MDM2_P2_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'BAX_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'h1433s_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'AIP1_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'GADD45_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'NOXA_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'raw data' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'P53R2_TA'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'WAF1nWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'MDM2nWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'BAXnWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'h1433snWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'AIP1nWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'PUMAnWT_Saos2'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'normalized percent wild-type' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'FUNCTION_ISHIOKA', @level2type=N'COLUMN',@level2name=N'SubG1nWT_Saos2'
GO
